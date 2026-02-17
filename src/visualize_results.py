import argparse
import os
import platform

import matplotlib

# バックグラウンド実行時のフリーズ防止
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# 日本語フォント設定 (Windows)
plt.rcParams["font.family"] = "MS Gothic"


def configure_fonts():
    os_name = platform.system()
    if os_name == "Windows":
        font_name = "Meiryo"
    elif os_name == "Darwin":
        font_name = "Hiragino Sans"
    else:
        font_name = "IPAGothic"
    try:
        plt.rcParams["font.family"] = font_name
    except Exception:
        pass


def plot_sector_returns(df, cols, output_path, title):
    """
    Generic function to plot sector returns as a bar chart.
    """
    # Filter cols that exist
    existing_cols = [c for c in cols if c in df.columns]
    if not existing_cols:
        print(f"No columns found for plot: {title}")
        return

    # Melt
    df_melted = df.melt(
        id_vars=["SectorName"],
        value_vars=existing_cols,
        var_name="Period",
        value_name="Return",
    )

    # Clean up Period names for legend if needed
    name_map = {
        "PrevFY_Change": "Previous FY",
        "YTD_Change": "Latest YTD",
        "Latest_PriceChange": "Latest Day",
        "YTD_Change_6mo": "6 Months Ago",
        "YTD_Change_3mo": "3 Months Ago",
        "YTD_Change_1mo": "1 Month Ago",
        "YTD_Change_PrevWeekend": "Prev Weekend",
    }
    df_melted["Period"] = df_melted["Period"].map(lambda x: name_map.get(x, x))

    plt.figure(figsize=(14, 12))
    sns.set_theme(style="whitegrid")
    configure_fonts()

    # Create palette
    periods = df_melted["Period"].unique()
    palette = sns.color_palette("husl", len(periods))

    # Draw Bar Chart
    ax = sns.barplot(
        data=df_melted, x="Return", y="SectorName", hue="Period", palette=palette
    )

    # Format X axis
    vals = ax.get_xticks()
    ax.set_xticklabels([f"{x:.0%}" for x in vals])

    plt.title(title, fontsize=16)
    plt.xlabel("Return (%)", fontsize=12)
    plt.ylabel("Sector", fontsize=12)
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()

    plt.savefig(output_path)
    print(f"Chart saved: {output_path}")
    plt.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", required=True, help="Path to input CSV file")
    parser.add_argument(
        "--output-png", required=True, help="Path to output PNG file (Graph 1)"
    )
    parser.add_argument(
        "--output-timeline-png",
        required=False,
        help="Path to output PNG file (Graph 2)",
    )
    args = parser.parse_args()

    csv_file = args.input_csv
    output_img = args.output_png
    output_timeline = args.output_timeline_png

    if not os.path.exists(csv_file):
        print(f"File not found: {csv_file}")
        return

    df = pd.read_csv(csv_file)

    # Sort by YTD Change for better visualization (common for both)
    if "YTD_Change" in df.columns:
        df = df.sort_values("YTD_Change", ascending=False)

    # Graph 1: Structure vs Trend vs Latest (Existing)
    cols_g1 = ["PrevFY_Change", "YTD_Change", "Latest_PriceChange"]
    plot_sector_returns(
        df, cols_g1, output_img, "Sector Performance: Structure vs Trend vs Latest"
    )

    # Graph 2: YTD Return Timeline (New)
    if output_timeline:
        cols_g2 = [
            "YTD_Change_6mo",
            "YTD_Change_3mo",
            "YTD_Change_1mo",
            "YTD_Change_PrevWeekend",
            "YTD_Change",
        ]
        plot_sector_returns(df, cols_g2, output_timeline, "Sector YTD Return Timeline")


if __name__ == "__main__":
    main()
