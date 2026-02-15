import argparse
import os
from datetime import datetime

import pandas as pd


def generate_html(
    csv_path,
    output_html_path,
    image_rel_path,
    timeline_image_rel_path=None,
    rotation_image_rel_path=None,
    heatmap_image_rel_path=None,
):
    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}")
        return

    df = pd.read_csv(csv_path)

    # Sort by YTD Change desc
    if "YTD_Change" in df.columns:
        df_sorted = df.sort_values("YTD_Change", ascending=False)
    else:
        df_sorted = df

    # Extract Market Context Info
    market_trend = "Unknown"
    if "Market_Trend" in df.columns:
        market_trend = df.iloc[0]["Market_Trend"] if not df.empty else "Unknown"

    # Helper for formatting percentage with color
    def fmt_pct(val, key=None):
        try:
            v = float(val)
            if pd.isna(v):
                return "-", "neutral"
            s = f"{v:.2%}"
            cls = "positive" if v > 0 else "negative" if v < 0 else "neutral"
            return s, cls
        except (ValueError, TypeError):
            return "-", "neutral"

    # --- Generate Table 1 Rows ---
    rows_table1 = ""
    for _, row in df_sorted.iterrows():
        sector_code = row.get("SectorCode", "")
        sector_name = row.get("SectorName", "")

        prev_fy_str, prev_fy_cls = fmt_pct(row.get("PrevFY_Change"))
        ytd_str, ytd_cls = fmt_pct(row.get("YTD_Change"))
        latest_p_str, latest_p_cls = fmt_pct(row.get("Latest_PriceChange"))
        latest_v_str, latest_v_cls = fmt_pct(row.get("Latest_VolumeChange"))
        vs_topix_str, vs_topix_cls = fmt_pct(row.get("VsTOPIX"))  # VsTOPIX_latest

        # Trading Value (Billions JPY)
        try:
            tv = float(row.get("TradingValue", 0))
            tv_str = f"{tv / 100000000:.1f} 億円"
        except (ValueError, TypeError):
            tv_str = "-"

        status_rotation = row.get("Status_Rotation", "-")
        # Sector Trend
        sector_trend = row.get("Sector_Trend", "-")

        rows_table1 += f"""
        <tr>
            <td>{sector_code}</td>
            <td class="sector-name">{sector_name}</td>
            <td class="{prev_fy_cls}">{prev_fy_str}</td>
            <td class="{ytd_cls}" style="background: rgba(255,255,255,0.05); font-weight:bold;">{ytd_str}</td>
            <td class="{vs_topix_cls}">{vs_topix_str}</td>
            <td class="{latest_p_cls}">{latest_p_str}</td>
            <td class="{latest_v_cls}">{latest_v_str}</td>
            <td>{tv_str}</td>
            <td>{status_rotation}</td>
            <td>{sector_trend}</td>
        </tr>
        """
        # Note: StockCount column removed as requested

    # --- Generate Table 2 Rows ---
    rows_table2 = ""
    # Define metrics order: 6mo, 3mo, 1mo, 1W (PrevWeekend), Latest
    # Changed based on update: 1w added
    time_points = [
        ("6mo", "6 Months Ago"),
        ("3mo", "3 Months Ago"),
        ("1mo", "1 Month Ago"),
        ("1w", "1 Week Ago"),
        ("", "Latest"),  # Empty suffix for latest YTD_Change
    ]

    for _, row in df_sorted.iterrows():
        sector_code = row.get("SectorCode", "")
        sector_name = row.get("SectorName", "")

        # Build cells for YTD, Rs (VsTOPIX), VolChg
        cells = ""

        # 1. YTD Return
        for suffix, _ in time_points:
            col = f"YTD_Change_{suffix}" if suffix else "YTD_Change"
            s, c = fmt_pct(row.get(col))
            cells += f'<td class="{c}">{s}</td>'

        # 2. Vs TOPIX (RS)
        for suffix, _ in time_points:
            # Suffix mapping
            if suffix == "":
                key = "VsTOPIX"
            elif suffix == "1w":
                # check VsTOPIX_1w or RS_1w?
                # industry_analysis output VsTOPIX_1w_window (if mapped)
                # Actually industry_analysis uses mapped keys: 1w -> 1w_window
                # And outputs VsTOPIX_1w
                # Also market_context output RS_1w
                # Let's use RS if available, else VsTOPIX
                key = "RS_1w" if "RS_1w" in row else "VsTOPIX_1w"
            else:
                key = f"RS_{suffix}" if f"RS_{suffix}" in row else f"VsTOPIX_{suffix}"

            s, c = fmt_pct(row.get(key))
            cells += f'<td class="{c}">{s}</td>'

        rows_table2 += f"""
        <tr>
            <td>{sector_code}</td>
            <td class="sector-name">{sector_name}</td>
            {cells}
        </tr>
        """

    # Ensure output directory exists
    output_dir = os.path.dirname(output_html_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    timeline_img_html = ""
    if timeline_image_rel_path:
        timeline_img_html = f"""
        <div class="chart-container">
            <h3>Timeline Analysis</h3>
            <img src="{timeline_image_rel_path}" alt="Timeline Chart">
        </div>
        """

    context_img_html = ""
    if rotation_image_rel_path and heatmap_image_rel_path:
        context_img_html = f"""
        <div class="context-container" style="display:flex; justify-content:space-around; flex-wrap:wrap;">
            <div style="flex:1; min-width:400px; padding:10px;">
                <h3>Sector Rotation (RS 3M vs 1M)</h3>
                <img src="{rotation_image_rel_path}" alt="Sector Rotation">
            </div>
            <div style="flex:1; min-width:400px; padding:10px;">
                <h3>Sector Heatmap (1W - 6M)</h3>
                <img src="{heatmap_image_rel_path}" alt="Sector Heatmap">
            </div>
        </div>
        """

    html_template = f"""
<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>J-Quants Sector 3-Timeframe Analysis</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&family=Noto+Sans+JP:wght@400;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-color: #0f172a;
            --text-main: #e2e8f0;
            --positive: #4ade80;
            --negative: #f87171;
            --neutral: #94a3b8;
        }}
        body {{
            font-family: 'Inter', 'Noto Sans JP', sans-serif;
            background-color: var(--bg-color);
            color: var(--text-main);
            padding: 2rem;
            max-width: 1600px;
            margin: 0 auto;
        }}
        h1 {{ text-align: center; color: #38bdf8; }}
        h2, h3 {{ color: #e2e8f0; margin-top: 2rem; border-bottom: 1px solid #334155; padding-bottom: 0.5rem; }}

        table {{ width: 100%; border-collapse: collapse; margin-top: 1rem; font-size: 0.85em; }}
        th, td {{ padding: 0.5rem 0.75rem; text-align: right; border-bottom: 1px solid #334155; }}
        th {{ text-align: center; color: #94a3b8; font-weight: 600; background: #1e293b; }}
        td:nth-child(2) {{ text-align: left; min-width: 120px; }} /* Sector Name */
        td:first-child {{ text-align: center; }} /* Code */

        .positive {{ color: var(--positive); }}
        .negative {{ color: var(--negative); }}
        .neutral {{ color: var(--neutral); }}

        .chart-container {{ text-align: center; margin: 2rem 0; }}
        img {{ max-width: 100%; border-radius: 8px; border: 1px solid #334155; }}

        /* Table 2 specifics */
        .group-header {{ border-bottom: 2px solid #475569; }}

        .market-context-box {{
            background: #1e293b;
            padding: 1.5rem;
            border-radius: 8px;
            margin-bottom: 2rem;
            border: 1px solid #334155;
        }}
        .market-trend {{
             font-size: 1.2em;
             font-weight: bold;
             color: #fca5a5; /* Light Red */
        }}
    </style>
</head>
<body>
    <h1>J-Quants Sector Analysis</h1>
    <p style="text-align:center">Generated at {datetime.now().strftime("%Y-%m-%d %H:%M")}</p>

    <div class="market-context-box">
        <h2>Market Context</h2>
        <p>TOPIX Trend: <span class="market-trend">{market_trend}</span></p>
        {context_img_html}
    </div>

    <div class="chart-container">
        <h3>Summary Chart</h3>
        <img src="{image_rel_path}" alt="Chart">
    </div>

    {timeline_img_html}

    <h2>Table 1: Main Overview</h2>
    <table>
        <thead>
            <tr>
                <th>Code</th>
                <th>Sector</th>
                <th>Prev FY (Struct)</th>
                <th>Current YTD (Trend)</th>
                <th>Vs TOPIX</th>
                <th>Latest (React)</th>
                <th>Latest Vol Chg</th>
                <th>Trading Value</th>
                <th>Rotation</th>
                <th>Trend</th>
            </tr>
        </thead>
        <tbody>
            {rows_table1}
        </tbody>
    </table>

    <h2>Table 2: Time-Series Metrics (Returns & RS)</h2>
    <table>
        <thead>
            <tr>
                <th rowspan="2">Code</th>
                <th rowspan="2">Sector</th>
                <th colspan="5" class="group-header">YTD Return</th>
                <th colspan="5" class="group-header">Relative Strength (vs TOPIX)</th>
            </tr>
            <tr>
                <th>6mo</th><th>3mo</th><th>1mo</th><th>1w</th><th>Now</th>
                <th>6mo</th><th>3mo</th><th>1mo</th><th>1w</th><th>Now</th>
            </tr>
        </thead>
        <tbody>
            {rows_table2}
        </tbody>
    </table>
</body>
</html>
    """

    with open(output_html_path, "w", encoding="utf-8") as f:
        f.write(html_template)
    print(f"Report generated: {os.path.abspath(output_html_path)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", required=True, help="Input CSV path")
    parser.add_argument("--output-html", required=True, help="Output HTML path")
    parser.add_argument(
        "--image-path", default="../sector_returns.png", help="Relative path to image 1"
    )
    parser.add_argument(
        "--timeline-image-path",
        default=None,
        help="Relative path to image 2 (timeline)",
    )
    parser.add_argument("--rotation-image-path", default=None)
    parser.add_argument("--heatmap-image-path", default=None)
    args = parser.parse_args()

    generate_html(
        args.input_csv,
        args.output_html,
        args.image_path,
        args.timeline_image_path,
        args.rotation_image_path,
        args.heatmap_image_path,
    )
