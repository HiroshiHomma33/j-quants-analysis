import argparse
import logging
import os
import sys

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas.plotting import register_matplotlib_converters

# 親ディレクトリをパスに追加してjq_api_libsをインポート可能にする
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import analysis_lib
import config
from jq_api_libs import jq_api

register_matplotlib_converters()

# 日本語フォント設定 (Windows)
plt.rcParams["font.family"] = "MS Gothic"

# 出力ディレクトリ設定
OUTPUT_DIR = config.get_stocks_charts_dir()
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def create_stock_chart(df, code, name, output_path):
    """
    Create a chart with Price (Candle + MA) and Volume.
    """
    # Filter last 6 months (approx 120 days)
    # Ensure Date is datetime
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.sort_values("Date")

    # Slice last 120 days
    df = df.tail(120).copy()
    if df.empty:
        return

    # Convert dates to numbers for plotting (crucial for bar widths in days)
    df["DateNum"] = mdates.date2num(df["Date"])

    # Create Figure
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(10, 8), gridspec_kw={"height_ratios": [3, 1]}, sharex=True
    )

    # 1. Price Chart (Candlestick)

    # Moving Averages
    df["MA25"] = df["Close"].rolling(window=25).mean()
    df["MA75"] = df["Close"].rolling(window=75).mean()

    # Plot MA
    ax1.plot(df["DateNum"], df["MA25"], color="orange", label="MA25", linewidth=1.5)
    ax1.plot(df["DateNum"], df["MA75"], color="purple", label="MA75", linewidth=1.5)

    # Candlestick
    width = 0.6

    up = df[df.Close >= df.Open]
    down = df[df.Close < df.Open]

    col_up = "red"
    col_down = "blue"

    # Wicks (black or gray)
    ax1.vlines(df["DateNum"], df["Low"], df["High"], color="#555555", linewidth=1)

    # Up Bodies
    # Using DateNum ensures width=0.6 refers to 0.6 days
    ax1.bar(
        up["DateNum"],
        up["Close"] - up["Open"],
        bottom=up["Open"],
        color="white",
        edgecolor=col_up,
        width=width,
        linewidth=1,
    )

    # Down Bodies
    ax1.bar(
        down["DateNum"],
        down["Open"] - down["Close"],
        bottom=down["Close"],
        color=col_down,
        edgecolor=col_down,
        width=width,
        linewidth=1,
    )

    ax1.set_title(f"{code} {name}", fontsize=14, fontweight="bold")
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc="upper left")
    ax1.set_ylabel("Price")

    # 2. Volume Chart
    ax2.bar(df["DateNum"], df["Volume"], color="#aaaaaa", width=width)
    ax2.set_ylabel("Volume")
    ax2.grid(True, alpha=0.3)

    # Formatting Dates
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax2.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45)

    plt.tight_layout()
    try:
        fig.savefig(output_path)
        # Check size
        if os.path.exists(output_path) and os.path.getsize(output_path) < 5000:
            logging.warning(
                f"Generated chart {output_path} is suspiciously small ({os.path.getsize(output_path)} bytes)."
            )
    except Exception as e:
        logging.error(f"Failed to save chart {output_path}: {e}")
    finally:
        plt.close(fig)


def plot_relative_performance(
    stock_df, sector_df, topix_df, code, stock_name, output_path
):
    """
    株価、セクター、TOPIXの6ヶ月比較チャートを生成する (The Triad Comparison)
    """
    # 1. データ結合と整形
    # Dateカラムをdatetimeにしておく
    for d in [stock_df, sector_df, topix_df]:
        if d is not None and "Date" in d.columns:
            d["Date"] = pd.to_datetime(d["Date"])

    # マージ (Inner Joinで共通期間のみにする)
    # stock_df をベースにする
    df = stock_df[["Date", "Close"]].copy()
    df.columns = ["Date", "Close_stock"]

    if sector_df is not None and not sector_df.empty:
        sec = sector_df[["Date", "Close"]].copy()
        sec.columns = ["Date", "Close_sec"]
        df = pd.merge(df, sec, on="Date", how="inner")
    else:
        # セクターデータがない場合はダミー列 (NaN)
        df["Close_sec"] = np.nan

    if topix_df is not None and not topix_df.empty:
        top = topix_df[["Date", "Close"]].copy()
        top.columns = ["Date", "Close_topix"]
        df = pd.merge(df, top, on="Date", how="inner")
    else:
        df["Close_topix"] = np.nan

    # 直近120営業日にスライス
    df = df.sort_values("Date").tail(120).copy()

    if df.empty:
        logging.warning(
            f"[{code}] DF is empty after merge/slice. Skipping relative chart."
        )
        return

    # 2. 正規化（Normalize）: 起点を0%にする
    # iloc[0] が基準
    base_stock = df["Close_stock"].iloc[0]
    df["Stock_Norm"] = (df["Close_stock"] / base_stock - 1) * 100

    if not df["Close_sec"].isnull().all():
        base_sec = df["Close_sec"].iloc[0]
        df["Sector_Norm"] = (df["Close_sec"] / base_sec - 1) * 100
    else:
        df["Sector_Norm"] = np.nan

    if not df["Close_topix"].isnull().all():
        base_topix = df["Close_topix"].iloc[0]
        df["Topix_Norm"] = (df["Close_topix"] / base_topix - 1) * 100
    else:
        df["Topix_Norm"] = np.nan

    # 3. プロット作成
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(12, 10), gridspec_kw={"height_ratios": [3, 1]}, sharex=True
    )

    # --- 上段: 3線比較チャート ---
    ax1.set_title(
        f"{code} {stock_name}: vs Sector & TOPIX (6 Months)",
        fontsize=14,
        fontweight="bold",
    )

    # TOPIX (市場)
    if "Topix_Norm" in df.columns:
        ax1.plot(
            df["Date"],
            df["Topix_Norm"],
            color="#7f8c8d",
            linestyle="--",
            label="TOPIX",
            alpha=0.7,
            zorder=3,
        )

    # Sector (業種)
    if "Sector_Norm" in df.columns:
        ax1.plot(
            df["Date"],
            df["Sector_Norm"],
            color="#e67e22",
            label="Sector",
            linewidth=1.5,
            zorder=4,
        )

    # Stock (個別)
    ax1.plot(
        df["Date"],
        df["Stock_Norm"],
        color="#2980b9",
        label="Stock",
        linewidth=2.5,
        zorder=10,
    )

    # 強弱エリアの塗りつぶし (Stock vs Sector)
    if "Sector_Norm" in df.columns and not df["Sector_Norm"].isnull().all():
        ax1.fill_between(
            df["Date"],
            df["Stock_Norm"],
            df["Sector_Norm"],
            where=(df["Stock_Norm"] >= df["Sector_Norm"]),
            facecolor="#2ecc71",
            alpha=0.2,
            interpolate=True,
            label="Outperform Sector",
        )
        ax1.fill_between(
            df["Date"],
            df["Stock_Norm"],
            df["Sector_Norm"],
            where=(df["Stock_Norm"] < df["Sector_Norm"]),
            facecolor="#e74c3c",
            alpha=0.2,
            interpolate=True,
            label="Underperform Sector",
        )

    ax1.set_ylabel("Cumulative Return (%)")
    ax1.legend(loc="upper left")
    ax1.grid(True, linestyle=":", alpha=0.6)

    # --- 下段: 対TOPIX 相対強度(RS) ---
    # 単純な日次リターンの差分ではなく、累積リターンの差（Alpha）を表示
    if "Topix_Norm" in df.columns and not df["Topix_Norm"].isnull().all():
        df["Alpha_Trend"] = df["Stock_Norm"] - df["Topix_Norm"]

        colors = ["#3498db" if v >= 0 else "#e74c3c" for v in df["Alpha_Trend"]]
        ax2.bar(df["Date"], df["Alpha_Trend"], color=colors, alpha=0.8, width=0.8)

        ax2.axhline(0, color="black", linewidth=0.5)
        ax2.set_ylabel("Alpha vs TOPIX (%)")
        ax2.set_title("Relative Strength vs TOPIX", fontsize=10)
    else:
        ax2.text(
            0.5,
            0.5,
            "TOPIX Data Missing",
            transform=ax2.transAxes,
            ha="center",
            va="center",
        )

    ax2.grid(True, linestyle=":", alpha=0.5)

    # 日付フォーマット
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.xticks(rotation=0)

    plt.tight_layout()
    try:
        fig.savefig(output_path)
        if os.path.exists(output_path) and os.path.getsize(output_path) < 5000:
            logging.warning(
                f"Generated relative chart {output_path} is suspiciously small ({os.path.getsize(output_path)} bytes)."
            )
    except Exception as e:
        logging.error(f"Failed to save relative chart {output_path}: {e}")
    finally:
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-csv", required=True, help="Path to stock_analysis_results.csv"
    )
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--date", help="Date string (YYYYMMDD) to append to filenames")
    args = parser.parse_args()

    setup_logging()
    logging.info("=== Stock Visualization Started ===")

    if not os.path.exists(args.input_csv):
        logging.error(f"Input file not found: {args.input_csv}")
        sys.exit(1)

    df_results = pd.read_csv(args.input_csv)
    logging.info(f"Loaded {len(df_results)} stocks from CSV.")

    api_key = jq_api.get_api_key()

    # Pre-load TOPIX for Relative Charts
    # Use config date if possible, but we don't have it passed here easily unless we recalc.
    # Just load full available timeseries from local/API.
    # For now, simplistic load from analysis_lib
    date_config = analysis_lib.get_fiscal_year_config()  # Need for fallback API fetch
    df_topix = analysis_lib.load_topix_timeseries(api_key, date_config)
    logging.info(f"Loaded TOPIX data: {len(df_topix)} records")

    # Cache for Sector Indices: {sector_code: df_sector}
    sector_indices_cache = {}

    count = 0
    rel_count = 0
    for _, row in df_results.iterrows():
        code = str(row["Code"])
        name = row.get("Name", code)
        sector_code = str(row.get("SectorCode", ""))

        # Cleanup sector_code
        if sector_code and sector_code != "nan":
            try:
                sector_code_int = int(float(sector_code))
                sector_code = f"{sector_code_int:04d}"
            except ValueError:
                sector_code = ""

        # Load Stock Data
        df_stock = None
        # if args.test:
        #     df_stock = analysis_lib.fetch_stock_data_from_api(api_key, code)

        # Try local first (Production & Test if available)
        if df_stock is None:
            if sector_code:
                try:
                    df_stock = analysis_lib.load_stock_data_from_local(
                        code, sector_code
                    )
                except Exception:
                    pass

        # Fallback API if local missing
        if df_stock is None or df_stock.empty:
            logging.info(f"Local data missing for {code}, fetching API...")
            df_stock = analysis_lib.fetch_stock_data_from_api(api_key, code)

        if df_stock is None or df_stock.empty:
            logging.warning(f"No data for {code}. Skipping chart.")
            continue

        # --- 1. Standard Chart ---
        if args.date:
            chart_filename = f"chart_{code}_{args.date}.png"
        else:
            chart_filename = f"chart_{code}.png"
        output_path = os.path.join(OUTPUT_DIR, chart_filename)

        try:
            create_stock_chart(df_stock, code, name, output_path)
            logging.info(f"Generated chart for {code}: {output_path}")
            count += 1
        except Exception as e:
            logging.error(f"Failed to create chart for {code}: {e}")

        # --- 2. Relative Performance Chart ---
        if args.date:
            rel_chart_filename = f"chart_relative_{code}_{args.date}.png"
        else:
            rel_chart_filename = f"chart_relative_{code}.png"
        rel_output_path = os.path.join(OUTPUT_DIR, rel_chart_filename)

        try:
            # Get Sector Index
            df_sector = None
            if sector_code:
                if sector_code in sector_indices_cache:
                    df_sector = sector_indices_cache[sector_code]
                else:
                    logging.info(f"Calculating Sector Index for {sector_code}...")
                    df_sector = analysis_lib.get_sector_index(sector_code)
                    sector_indices_cache[sector_code] = df_sector

            plot_relative_performance(
                df_stock, df_sector, df_topix, code, name, rel_output_path
            )
            logging.info(f"Generated relative chart for {code}: {rel_output_path}")
            rel_count += 1
        except Exception as e:
            logging.error(f"Failed to create relative chart for {code}: {e}")

    logging.info(
        f"Visualization Completed. Created {count} standard charts, {rel_count} relative charts."
    )
    print(
        f"Created {count} standard charts, {rel_count} relative charts in {OUTPUT_DIR}"
    )


if __name__ == "__main__":
    main()
