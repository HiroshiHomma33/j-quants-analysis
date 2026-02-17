import argparse
import logging
import os
import sys

import numpy as np
import pandas as pd

# 親ディレクトリをパスに追加してjq_api_libsをインポート可能にする
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import analysis_lib
import config
from jq_api_libs import jq_api

# 出力ディレクトリ設定
# output_dir arg overrides this, but default is from config
OUTPUT_DIR = config.get_sector_data_dir()
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def setup_logging(output_dir=OUTPUT_DIR):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=os.path.join(output_dir, "industry_analysis.log"),
        filemode="w",
        encoding="utf-8",
        force=True,
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger("").addHandler(console)


def analyze_all_sectors(
    id_token, df_prime, date_config, topix_metrics, args_test=False
):
    # Process by sector
    sectors = (
        df_prime[["Sector33Code", "Sector33CodeName"]]
        .drop_duplicates()
        .sort_values("Sector33Code")
    )
    if args_test:
        sectors = sectors.head(3)

    results = []

    # Map for IssuedShares
    code_to_shares = {}
    use_weighted = False
    if "IssuedShares" in df_prime.columns:
        use_weighted = True
        for _, row in df_prime.iterrows():
            try:
                code_to_shares[row["Code"]] = float(row["IssuedShares"])
            except (ValueError, TypeError, KeyError):
                code_to_shares[row["Code"]] = 0.0

    for _, row in sectors.iterrows():
        sector_code = row["Sector33Code"]
        sector_name = row["Sector33CodeName"]
        logging.info(f"Analyzing Sector: {sector_name} ({sector_code})")

        target_df = df_prime[df_prime["Sector33Code"] == sector_code]
        codes = target_df["Code"].tolist()

        if args_test:
            # テストモード時は各セクターの最初の数銘柄だけにする (10銘柄)
            codes = codes[:10]

        sector_metrics_list = []

        for code in codes:
            if args_test:
                # テストモード: APIから直接取得
                df_stock = analysis_lib.fetch_stock_data_from_api(id_token, code)
                # API取得時のエラーハンドリング
                if df_stock is None or df_stock.empty:
                    logging.warning(
                        f"Data for Code {code} could not be fetched from API. Skipping."
                    )
                    continue
            else:
                # 通常モード: ローカルからデータ読み込み
                df_stock = analysis_lib.load_stock_data_from_local(code, sector_code)

                # データ取得チェック
                if df_stock is None or df_stock.empty:
                    # error_msg = f"Error: Data for Code {code} (Sector {sector_code}) not found in local DB."
                    # print(error_msg)
                    # logging.error(error_msg)
                    continue

            points = analysis_lib.extract_stock_points(df_stock, date_config)
            metrics = analysis_lib.calculate_stock_metrics(points)

            # Validity check
            if any(
                not np.isnan(v)
                for k, v in metrics.items()
                if k not in ["TradingValue", "Close"]
            ):
                metrics["Code"] = code
                if use_weighted and code in code_to_shares:
                    metrics["MarketCap"] = (
                        metrics.get("Close", 0) * code_to_shares[code]
                    )
                else:
                    metrics["MarketCap"] = 1.0
                sector_metrics_list.append(metrics)

        if not sector_metrics_list:
            logging.warning(
                f"No valid metrics found for sector {sector_name} ({sector_code})"
            )
            continue

        df_sector_metrics = pd.DataFrame(sector_metrics_list)

        # Weighted Average Logic
        def weighted_avg(col, df):
            if col not in df.columns:  # Handle missing columns safely
                return 0.0
            valid = df.dropna(subset=[col, "MarketCap"])
            if valid.empty or valid["MarketCap"].sum() == 0:
                return np.nan
            return np.average(valid[col], weights=valid["MarketCap"])

        res = {
            "SectorCode": sector_code,
            "SectorName": sector_name,
            "PrevFY_Change": weighted_avg("PrevFY_Change", df_sector_metrics),
            "YTD_Change": weighted_avg("YTD_Change", df_sector_metrics),
            "Latest_PriceChange": weighted_avg("Latest_PriceChange", df_sector_metrics),
            "Latest_VolumeChange": weighted_avg(
                "Latest_VolumeChange", df_sector_metrics
            ),
            "TradingValue": df_sector_metrics["TradingValue"].sum(),
            "StockCount": len(df_sector_metrics),
        }

        # Add averaged time-series metrics
        time_points = ["6mo", "3mo", "1mo", "1w", "prev_weekend"]
        for suffix in time_points:
            stock_suffix = suffix
            if suffix == "prev_weekend":
                stock_suffix = "PrevWeekend"

            col_ytd = f"YTD_Change_{stock_suffix}"
            col_vol = f"VolChange_{stock_suffix}"

            # Calculate Sector Weighted Avg
            res[col_ytd] = weighted_avg(col_ytd, df_sector_metrics)
            res[col_vol] = weighted_avg(col_vol, df_sector_metrics)

            # Period Returns (1mo, 3mo, 6mo)
            col_ret = f"Return_{suffix}"
            if col_ret in df_sector_metrics.columns:
                res[col_ret] = weighted_avg(col_ret, df_sector_metrics)
            else:
                res[col_ret] = np.nan

            # Calculate VsTOPIX (Sector YTD - TOPIX YTD)
            topix_key = f"YTD_Change_{suffix}"

            if topix_key in topix_metrics:
                res[f"VsTOPIX_{stock_suffix}"] = res[col_ytd] - topix_metrics[topix_key]
            else:
                res[f"VsTOPIX_{stock_suffix}"] = np.nan

        # Trend Status Aggregation
        if "Trend_Status" in df_sector_metrics.columns:
            counts = df_sector_metrics["Trend_Status"].value_counts()
            total = len(df_sector_metrics)
            uptrend_count = counts.get("Uptrend", 0)
            downtrend_count = counts.get("Downtrend", 0)

            res["Uptrend_Ratio"] = uptrend_count / total if total > 0 else 0
            res["Downtrend_Ratio"] = downtrend_count / total if total > 0 else 0

            # Determine Sector Trend based on Ratio
            if res["Uptrend_Ratio"] > 0.5:
                res["Sector_Trend"] = "Uptrend"
            elif res["Downtrend_Ratio"] > 0.5:
                res["Sector_Trend"] = "Downtrend"
            else:
                res["Sector_Trend"] = "Neutral"
        else:
            res["Sector_Trend"] = "Unknown"

        # Main VsTOPIX (Latest)
        res["VsTOPIX"] = res["YTD_Change"] - topix_metrics.get("YTD_Change", 0)

        prev = res["PrevFY_Change"]
        curr = res["YTD_Change"]
        if prev < 0 and curr > 0:
            status = "リバーサル"
        elif prev > 0 and curr > 0:
            status = "継続トレンド"
        elif prev > 0 and curr < 0:
            status = "失速"
        elif prev < 0 and curr < 0:
            status = "低迷"
        else:
            status = "中立"
        res["Status_Rotation"] = status

        results.append(res)

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true")
    # Added: Output directory argument
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        help="Directory to save analysis results",
    )
    args = parser.parse_args()

    # Ensure output directory exists (if changed from default or default logic)
    output_dir = args.output_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    setup_logging(output_dir)
    logging.info("=== Industry Analysis Started (Refactored Mode) ===")

    try:
        api_key = jq_api.get_api_key()

        logging.info("Fetching Listed Info...")
        df_listed = jq_api.get_listed_info(api_key)
        df_prime = df_listed[df_listed["MarketCode"] == "0111"].copy()
        logging.info(f"Prime Market Stocks: {len(df_prime)}")

        # Use analysis_lib for config and topix
        date_config = analysis_lib.get_fiscal_year_config()
        topix_metrics = analysis_lib.get_topix_data(api_key, date_config)

        results = analyze_all_sectors(
            api_key, df_prime, date_config, topix_metrics, args.test
        )

        if results:
            df_results = pd.DataFrame(results)
            # Use analysis_lib for signal assignment
            df_results = analysis_lib.assign_supply_demand_signal(df_results)

            # Determine Data Date from topix_metrics if available, else date_config
            if "LatestDate" in topix_metrics:
                data_date = topix_metrics["LatestDate"]
                print(f"Using Data Date from TOPIX: {data_date}")
            else:
                # Fallback to config window end date
                latest_end_date_str = date_config["latest_window"][1]
                data_date = latest_end_date_str.replace("-", "")
                print(f"Using Fallback Date: {data_date}")

            output_filename = f"sector_analysis_results_{data_date}.csv"
            output_path = os.path.join(output_dir, output_filename)

            if not args.test:
                df_results.to_csv(output_path, index=False)
                logging.info(f"Analysis Completed. Saved to {output_path}")
                print(f"Data analysis completed. Saved to {output_path}")

                # Output key-value pairs for the runner script to capture
                print(f"OUTPUT_CSV={os.path.abspath(output_path)}")
                print(f"DATA_DATE={data_date}")
            else:
                # In test mode, we also want to save CSV to verify downstream steps
                test_output_path = os.path.join(output_dir, f"test_{output_filename}")
                df_results.to_csv(test_output_path, index=False)
                logging.info(f"Test Analysis Completed. Saved to {test_output_path}")
                print(f"Test analysis completed. Saved to {test_output_path}")

                print(f"OUTPUT_CSV={os.path.abspath(test_output_path)}")
                print(f"DATA_DATE={data_date}")

                # Show sample output in log/stdout
                print("Test mode enabled. Sample output:")
                print(df_results.head())
        else:
            logging.error("No results generated.")
            sys.exit(1)

    except Exception as e:
        logging.critical(f"Fatal Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
