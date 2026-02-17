import argparse
import logging
import os
import sys

import pandas as pd

# 出力ディレクトリ設定
OUTPUT_DIR = "industry_analysis/stocks"
CHART_DIR = "industry_analysis/stocks/charts"  # Relative path from execution root?
# Actually, if the HTML is in industry_analysis/stocks/, then charts should be referenced as ./charts/chart_CODE.png


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def generate_html(df, output_path, chart_rel_path="charts", date_str=None):
    """
    Generate HTML report from DataFrame.
    """
    html_content = """
    <!DOCTYPE html>
    <html lang="ja">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Individual Stock Analysis Report</title>
        <style>
            body { font-family: 'Helvetica Neue', Arial, sans-serif; margin: 0; padding: 20px; background-color: #f4f4f9; color: #333; }
            h1 { text-align: center; color: #2c3e50; margin-bottom: 30px; }
            .container { max-width: 1200px; margin: 0 auto; }
            .stock-card { background: white; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); margin-bottom: 30px; padding: 20px; }
            .stock-header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #eee; padding-bottom: 10px; margin-bottom: 15px; }
            .stock-title { font-size: 1.5em; font-weight: bold; color: #2980b9; }
            .stock-meta { font-size: 0.9em; color: #7f8c8d; }
            .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-bottom: 20px; }
            .metric-box { background: #ecf0f1; padding: 10px; border-radius: 5px; text-align: center; }
            .metric-label { font-size: 0.8em; color: #7f8c8d; display: block; }
            .metric-value { font-size: 1.1em; font-weight: bold; color: #2c3e50; }
            .positive { color: #e74c3c; } /* Red for up */
            .negative { color: #3498db; } /* Blue for down */
            .chart-container { text-align: center; }
            img { max-width: 100%; height: auto; border: 1px solid #ddd; border-radius: 4px; }
            .summary-table { width: 100%; border-collapse: collapse; margin-bottom: 40px; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
            .summary-table th, .summary-table td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
            .summary-table th { background-color: #2980b9; color: white; }
            .summary-table tr:hover { background-color: #f1f1f1; }
            .signal-badge { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 0.8em; color: white; background-color: #95a5a6; }
            .signal-buy { background-color: #e74c3c; }
            .signal-weak-buy { background-color: #f39c12; }
            .signal-neutral { background-color: #95a5a6; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Individual Stock Analysis Report</h1>

            <h2>Summary (Top 20)</h2>
            <table class="summary-table">
                <thead>
                    <tr>
                        <th>Code</th>
                        <th>Name</th>
                        <th>Sector</th>
                        <th>Sec Trend</th>
                        <th>Prev FY</th>
                        <th>YTD Change</th>
                        <th>Vol Change</th>
                        <th>Signal</th>
                        <th>Vs TOPIX</th>
                    </tr>
                </thead>
                <tbody>
    """

    # Add Summary Rows
    for _, row in df.iterrows():
        code = row["Code"]
        name = row.get("Name", "")
        sector = row.get("SectorCode", "")
        sector_trend = row.get("Sector_Trend", "-")

        prev_fy = float(row.get("PrevFY_Change", 0))
        ytd = float(row.get("YTD_Change", 0))
        vol = float(row.get("Latest_VolumeChange", 0))
        signal = row.get("Status_Signal", "-")
        vs_topix = float(row.get("VsTOPIX", 0))

        prev_class = "positive" if prev_fy > 0 else "negative"
        ytd_class = "positive" if ytd > 0 else "negative"
        vs_class = "positive" if vs_topix > 0 else "negative"

        sig_class = "signal-neutral"
        if "買い" in signal:
            sig_class = "signal-buy"
        elif "上昇" in signal:
            sig_class = "signal-weak-buy"

        html_content += f"""
                    <tr>
                        <td>{code}</td>
                        <td>{name}</td>
                        <td>{sector}</td>
                        <td>{sector_trend}</td>
                        <td class="{prev_class}">{prev_fy:+.2%}</td>
                        <td class="{ytd_class}">{ytd:+.2%}</td>
                        <td>{vol:+.2%}</td>
                        <td><span class="signal-badge {sig_class}">{signal}</span></td>
                        <td class="{vs_class}">{vs_topix:+.2%}</td>
                    </tr>
        """

    html_content += """
                </tbody>
            </table>

            <h2>Detailed Analysis</h2>
    """

    # Add Stock Cards
    for _, row in df.iterrows():
        code = row["Code"]
        name = row.get("Name", "")
        prev_fy = float(row.get("PrevFY_Change", 0))
        ytd = float(row.get("YTD_Change", 0))
        vol = float(row.get("Latest_VolumeChange", 0))
        close = row.get("Close", 0)
        signal = row.get("Status_Signal", "-")

        prev_str = f"{prev_fy:+.2%}"
        ytd_str = f"{ytd:+.2%}"
        vol_str = f"{vol:+.2%}"
        close_str = f"{close:,.0f}"

        rs_sector = row.get("RS_Sector_1mo")
        try:
            rs_sector = float(rs_sector)
            rs_sector_str = f"{rs_sector:+.2%}"
        except (ValueError, TypeError):
            rs_sector_str = "-"

        # Define chart filename
        if date_str:
            chart_filename = f"chart_{code}_{date_str}.png"
        else:
            chart_filename = f"chart_{code}.png"

        if chart_rel_path:
            chart_path = f"{chart_rel_path}/{chart_filename}"
        else:
            chart_path = f"charts/{chart_filename}"

        prev_class = "positive" if prev_fy > 0 else "negative"
        ytd_class = "positive" if ytd > 0 else "negative"

        html_content += f"""
            <div class="stock-card">
                <div class="stock-header">
                    <span class="stock-title">{code} {name}</span>
                    <span class="stock-meta">Signal: <strong>{signal}</strong></span>
                </div>
                <div class="metrics-grid">
                    <div class="metric-box">
                        <span class="metric-label">Close Price</span>
                        <span class="metric-value">{close_str}</span>
                    </div>
                    <div class="metric-box">
                        <span class="metric-label">Prev FY</span>
                        <span class="metric-value {prev_class}">{prev_str}</span>
                    </div>
                        <span class="metric-label">YTD Change</span>
                        <span class="metric-value {ytd_class}">{ytd_str}</span>
                    </div>
                    <div class="metric-box">
                        <span class="metric-label">Vol Change</span>
                        <span class="metric-value">{vol_str}</span>
                    </div>
                    <div class="metric-box">
                         <span class="metric-label">RS (vs Sec 1M)</span>
                         <span class="metric-value">{rs_sector_str}</span>
                    </div>
                </div>
                </div>
                <div class="chart-container">
                    <img src="{chart_path}" alt="Price/Volume Chart for {code}" loading="lazy">
                </div>
                <!-- Relative Chart -->
                <div class="chart-container" style="margin-top: 20px;">
                    <img src="{chart_path.replace("chart_", "chart_relative_")}" alt="Relative Performance Chart for {code}" loading="lazy">
                </div>
            </div>
        """

    html_content += """
        </div>
    </body>
    </html>
    """

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-csv", required=True, help="Path to stock_analysis_results.csv"
    )
    parser.add_argument("--output-html", required=True, help="Path to output HTML file")
    parser.add_argument(
        "--chart-rel-path",
        default="charts",
        help="Relative path to charts directory from HTML file",
    )
    parser.add_argument("--date", help="Date string (YYYYMMDD) for chart filenames")
    args = parser.parse_args()

    setup_logging()
    logging.info("=== Stock Report Generation Started ===")

    if not os.path.exists(args.input_csv):
        logging.error(f"Input file not found: {args.input_csv}")
        sys.exit(1)

    df = pd.read_csv(args.input_csv)
    logging.info(f"Loaded {len(df)} records.")

    try:
        generate_html(df, args.output_html, args.chart_rel_path, args.date)
        logging.info(f"Report generated: {args.output_html}")
        print(f"Report generated at {args.output_html}")
    except Exception as e:
        logging.error(f"Failed to generate report: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
