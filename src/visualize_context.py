import os

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Try to set Japanese font
try:
    # Common windows Japanese fonts
    font_path = "C:/Windows/Fonts/meiryo.ttc"
    if os.path.exists(font_path):
        fm.fontManager.addfont(font_path)
        plt.rcParams["font.family"] = "Meiryo"
    else:
        plt.rcParams["font.family"] = "Yu Gothic"
except Exception:
    pass


def plot_sector_rotation(df_context, output_path):
    """
    セクターローテーションマップを描画する (Scatter Plot)
    X軸: RS_3M (中期的な強さ)
    Y軸: RS_1M (短期的な勢い)
    """
    if df_context is None or df_context.empty:
        return

    # 必要なカラムがあるか確認
    if "RS_3mo" not in df_context.columns or "RS_1mo" not in df_context.columns:
        print("Required columns (RS_3mo, RS_1mo) missing for Sector Rotation Plot.")
        return

    plt.figure(figsize=(12, 10))

    # Grid
    plt.axhline(0, color="gray", linestyle="--", linewidth=1)
    plt.axvline(0, color="gray", linestyle="--", linewidth=1)

    # Scatter
    sns.scatterplot(
        data=df_context,
        x="RS_3mo",
        y="RS_1mo",
        hue="SectorName",
        s=200,
        legend=False,
        palette="viridis",
    )

    # Labels
    # Annotate top/bottom performers to avoid clutter
    # Or annotate all if not too many overlap. 33 sectors is a bit crowded.
    # Let's annotate all for now but use small font.
    # Let's annotate all for now but use small font.
    for _, row in df_context.iterrows():
        if pd.isna(row["RS_3mo"]) or pd.isna(row["RS_1mo"]):
            continue
        plt.text(
            row["RS_3mo"],
            row["RS_1mo"] + 0.002,
            row["SectorName"],
            horizontalalignment="center",
            size="small",
            color="black",
            weight="normal",
        )

    # Quadrant Labels
    # Q1: Leading (RS3M > 0, RS1M > 0)
    # Q2: Improving (RS3M < 0, RS1M > 0)
    # Q3: Lagging (RS3M < 0, RS1M < 0)
    # Q4: Weakening (RS3M > 0, RS1M < 0)

    # Calculate limits for placing labels
    # x_max = df_context["RS_3mo"].max()
    # y_max = df_context["RS_1mo"].max()

    # Just simple titles for quadrants
    plt.title("Sector Rotation Map (Relative Strength vs TOPIX)", fontsize=16)
    plt.xlabel("Medium-Term Relative Strength (3 Months)", fontsize=12)
    plt.ylabel("Short-Term Relative Strength (1 Month)", fontsize=12)

    # Add Quadrant Annotations (Background text?)
    # plt.text(x_max/2, y_max/2, "Leading", fontsize=20, color='green', alpha=0.1, ha='center')

    plt.grid(True, linestyle=":", alpha=0.6)
    plt.tight_layout()

    plt.savefig(output_path, dpi=100)
    plt.close()
    print(f"Sector Rotation Map saved to {output_path}")


def plot_sector_heatmap(df_context, output_path):
    """
    期間別リターンのヒートマップを描画する
    """
    if df_context is None or df_context.empty:
        return

    cols = ["Return_1w", "Return_1mo", "Return_3mo", "Return_6mo"]
    # Check if cols exist
    valid_cols = [c for c in cols if c in df_context.columns]

    if not valid_cols:
        print("No return columns found for Heatmap.")
        return

    # Prepare Data
    # Index: SectorName
    df_heat = df_context.set_index("SectorName")[valid_cols]

    # Sort by 1w return
    if "Return_1w" in valid_cols:
        df_heat = df_heat.sort_values("Return_1w", ascending=False)

    plt.figure(figsize=(10, 15))

    sns.heatmap(
        df_heat,
        annot=True,
        fmt=".1%",
        cmap="RdYlGn",
        center=0,
        cbar_kws={"label": "Return"},
    )

    plt.title("Sector Returns Heatmap", fontsize=16)
    plt.tight_layout()

    plt.savefig(output_path, dpi=100)
    plt.close()
    print(f"Sector Heatmap saved to {output_path}")
