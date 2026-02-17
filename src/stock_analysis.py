import argparse
import logging
import os
import sys
from datetime import datetime

import pandas as pd

# 親ディレクトリをパスに追加してjq_api_libsをインポート可能にする
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import analysis_lib
import config
from jq_api_libs import jq_api

# 出力ディレクトリ設定
OUTPUT_DIR = config.get_stocks_data_dir()
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def setup_logging(output_dir=OUTPUT_DIR):
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=os.path.join(output_dir, "stock_analysis.log"),
        filemode="w",
        encoding="utf-8",
        force=True,
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger("").addHandler(console)


def get_target_codes(api_key, code_list_path=None, test_mode=False):
    """
    分析対象の銘柄リストを取得する。
    - code_list_path指定時: そのCSVに含まれる銘柄コード
    - 指定なし: 東証プライム全銘柄 (テストモード時は10銘柄)
    """
    if code_list_path:
        if os.path.exists(code_list_path):
            logging.info(f"Reading target codes from {code_list_path}")
            try:
                # CSV: Assume single column or column named "Code"
                df = pd.read_csv(code_list_path, dtype=str)
                if "Code" in df.columns:
                    codes = df["Code"].tolist()
                else:
                    codes = df.iloc[:, 0].tolist()
                return codes
            except Exception as e:
                logging.error(f"Failed to read code list: {e}")
                sys.exit(1)
        else:
            logging.error(f"Code list file not found: {code_list_path}")
            sys.exit(1)

    logging.info("Fetching Prime Market list from API...")
    df_listed = jq_api.get_listed_info(api_key)
    # MarketCode "0111" = Prime
    df_prime = df_listed[df_listed["MarketCode"] == "0111"]
    codes = df_prime["Code"].tolist()

    if test_mode:
        logging.info("Test mode: limiting to 10 stocks")
        return codes[:10]

    return codes


def rank_and_filter_stocks(results, limit=20):
    """
    分析結果から有望銘柄を抽出しランキング化する。
    基準:
    1. YTD_Change (年初来騰落率) 降順
    2. フィルタ:
       - YTD_Change > 0 (プラス圏)
       - Latest_VolumeChange > -0.5 (出来高が極端に減っていない)
    """
    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)

    # Convert numeric columns safely
    cols = ["YTD_Change", "Latest_VolumeChange", "TradingValue", "Close"]
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Filter
    # 1. YTD Positive
    # 2. Volume Change not crashed (optional, lenient filter)
    filtered = df[
        (df["YTD_Change"] > 0)
        # & (df["Latest_VolumeChange"] > -0.5) # Comment out for now to see more results
    ].copy()

    if filtered.empty:
        logging.warning("No stocks matched filtering criteria. Returning raw top YTD.")
        filtered = df.copy()

    # Sort by YTD Change descending
    ranked = filtered.sort_values("YTD_Change", ascending=False)

    # Take top N
    return ranked.head(limit)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--code-list", help="Path to CSV file containing stock codes")
    parser.add_argument("--sector-csv", help="Path to Sector Analysis Results CSV")
    args = parser.parse_args()

    setup_logging()
    logging.info("=== Individual Stock Analysis Started ===")

    try:
        api_key = jq_api.get_api_key()

        # 1. Get Target Codes
        codes = get_target_codes(api_key, args.code_list, args.test)
        logging.info(f"Target Stocks: {len(codes)}")

        # 2. Config & TOPIX
        date_config = analysis_lib.get_fiscal_year_config()
        topix_metrics = analysis_lib.get_topix_data(api_key, date_config)

        # 3. Analyze Data
        # Map Code -> Sector/Name for local file loading and reporting
        logging.info("Fetching Listed Info for Sector mapping...")
        df_listed = jq_api.get_listed_info(api_key)
        code_to_sector = dict(
            zip(df_listed["Code"], df_listed["Sector33Code"], strict=True)
        )
        code_to_name = dict(
            zip(df_listed["Code"], df_listed["CompanyName"], strict=True)
        )

        results = []
        processed_count = 0

        # Load Sector Data if available
        sector_csv_path = None
        if args.sector_csv:
            sector_csv_path = args.sector_csv
        else:
            # Try to find latest automatically using analysis_lib
            found = analysis_lib.get_latest_sector_csv()
            if found:
                sector_csv_path = str(found)

        sector_data_map = {}
        if sector_csv_path and os.path.exists(sector_csv_path):
            logging.info(f"Loading Sector Context from {sector_csv_path}")
            try:
                df_sector = pd.read_csv(sector_csv_path, dtype={"SectorCode": str})
                # Create map: SectorCode -> dict of metrics
                # We want Sector_Trend, Return_1mo, etc.
                for _, row in df_sector.iterrows():
                    sc = str(row["SectorCode"])
                    sector_data_map[sc] = row.to_dict()
            except Exception as e:
                logging.warning(f"Failed to load sector CSV: {e}")

        for i, code in enumerate(codes):
            if i % 100 == 0:
                logging.info(f"Processed {i}/{len(codes)} stocks...")
            # Get Sector Code
            # Ensure code is string matching the keys in code_to_sector
            # listed_info Code is typically "13010". user input "9984" might need to be "99840"?
            # Actually J-Quants listed info codes are 5 digits (including check digit?) or just 4?
            # get_listed_info returns whatever the API returns.
            # Let's try flexible lookup.

            sector_code = code_to_sector.get(code)

            # Fallback: Try adding '0' if length is 4 (standard Japan code to 5 digit)
            if not sector_code and len(code) == 4:
                sector_code = code_to_sector.get(code + "0")
                if sector_code:
                    code = (
                        code + "0"
                    )  # Update 'code' to match the system's expected key

            if not sector_code:
                # If still not found, maybe it's int vs str issue, but we ensure string.
                # Just skip with warning.
                logging.warning(
                    f"Sector code not found for {code}. Keys sample: {list(code_to_sector.keys())[:5]}"
                )
                continue

            # Data Load
            if args.test:
                df_stock = analysis_lib.fetch_stock_data_from_api(api_key, code)
            else:
                df_stock = analysis_lib.load_stock_data_from_local(code, sector_code)

            if df_stock is None or df_stock.empty:
                continue

            # Analysis
            points = analysis_lib.extract_stock_points(df_stock, date_config)
            metrics = analysis_lib.calculate_stock_metrics(points)

            # Add Basic Info
            metrics["Code"] = code
            metrics["Name"] = code_to_name.get(code, "")
            metrics["SectorCode"] = sector_code

            # Add Sector Context
            if sector_code in sector_data_map:
                s_metrics = sector_data_map[sector_code]
                metrics["Sector_Trend"] = s_metrics.get("Sector_Trend", "-")

                # Calculate Relative Strength vs Sector (1 Month)
                # Stock Return 1M - Sector Return 1M
                if "Return_1mo" in metrics:
                    s_ret_1m = s_metrics.get("Return_1mo")
                    # Check if s_ret_1m is valid number
                    try:
                        s_ret_1m = float(s_ret_1m)
                        if not pd.isna(s_ret_1m) and not pd.isna(metrics["Return_1mo"]):
                            metrics["RS_Sector_1mo"] = metrics["Return_1mo"] - s_ret_1m
                    except (ValueError, TypeError):
                        pass

            # VsTOPIX
            if "YTD_Change" in metrics:
                metrics["VsTOPIX"] = metrics["YTD_Change"] - topix_metrics.get(
                    "YTD_Change", 0
                )

            results.append(metrics)
            processed_count += 1
            if processed_count % 100 == 0:
                logging.info(f"Analyzed {processed_count} stocks")

        # 4. Ranking / Filtering
        if results:
            logging.info(f"Total processed: {len(results)}")

            # If code list provided, return all analysis results (no filter)
            # If NO code list provided (scan mode), pick Top 20
            if args.code_list:
                df_final = pd.DataFrame(results)
                logging.info("Code list provided. Returning all analyzed stocks.")
            else:
                logging.info("No code list. Filtering for Top 20 Momentum Stocks.")
                df_final = rank_and_filter_stocks(results, limit=20)

            # Signals
            df_final = analysis_lib.assign_supply_demand_signal(df_final)

            # Save
            if "LatestDate" in topix_metrics:
                data_date = topix_metrics["LatestDate"]
            else:
                data_date = datetime.now().strftime("%Y%m%d")

            if args.code_list:
                filename = f"stock_analysis_results_indiv_{data_date}.csv"
            else:
                filename = f"stock_analysis_results_{data_date}.csv"
            out_path = os.path.join(OUTPUT_DIR, filename)
            df_final.to_csv(out_path, index=False)

            print(f"Stock analysis completed. Saved to {out_path}")
            print(f"OUTPUT_CSV={os.path.abspath(out_path)}")
        else:
            logging.warning("No results generated.")
            print("No results generated.")

    except Exception as e:
        logging.critical(f"Fatal Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
