import argparse
import os
import subprocess
import sys
from datetime import datetime

from src import config

PYTHON_EXEC = sys.executable


def run_script(script_name, args=None, capture_output=True):
    cmd = [PYTHON_EXEC, script_name]
    if args:
        cmd.extend(args)

    print(f"--- Running {script_name} with args: {args} ---")
    result = subprocess.run(
        cmd, capture_output=capture_output, text=True, encoding="utf-8"
    )

    if result.returncode != 0:
        print(f"Error running {script_name}:")
        print(result.stderr)
        raise RuntimeError(f"{script_name} failed.")

    print(result.stdout)
    return result.stdout


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--test", action="store_true", help="Run in test mode (10 stocks)"
    )
    parser.add_argument("--code-list", help="Path to CSV file with target codes")
    args = parser.parse_args()

    # Ensure directories via config
    config.ensure_directories()

    # Stocks config dirs
    # STOCKS_DATA_DIR = config.get_stocks_data_dir() # Used implicitly by stock_analysis.py
    # STOCKS_CHARTS_DIR = config.get_stocks_charts_dir() # Used implicitly by visualize_stocks.py
    STOCKS_RANK_DIR = config.get_stocks_rank_dir()
    STOCKS_INDIV_DIR = config.get_stocks_indiv_dir()

    # 1. Stock Analysis
    analysis_args = []
    if args.test:
        analysis_args.append("--test")
    if args.code_list:
        analysis_args.extend(["--code-list", args.code_list])

    try:
        output_data = run_script("src/stock_analysis.py", analysis_args)

        # Parse output to find CSV path
        # Expected stdout: "OUTPUT_CSV=..."
        csv_path = None

        for line in output_data.splitlines():
            if line.startswith("OUTPUT_CSV="):
                csv_path = line.split("=", 1)[1].strip()
            # If stock_analysis outputs date, capture it. It doesn't currently output DATA_DATE explicitly?
            # actually industry_analysis does, checking stock_analysis.py...
            # It prints "OUTPUT_CSV=..." but not DATA_DATE explicitly in my last view.
            # But filename has date.

        if not csv_path or not os.path.exists(csv_path):
            print("Error: Could not determine output CSV path from stock_analysis.py")
            sys.exit(1)

        print(f"Analysis CSV: {csv_path}")

        # Extract date from filename
        # CSV format: stock_analysis_results_YYYYMMDD.csv
        file_name = os.path.basename(csv_path)
        try:
            # part after "results_" and before ".csv"
            date_part = file_name.split("results_")[1].split(".")[0]
        except Exception:
            date_part = datetime.now().strftime("%Y%m%d")

        # 2. Visualize
        viz_args = ["--input-csv", csv_path, "--date", date_part]
        if args.test:
            viz_args.append("--test")

        run_script("src/visualize_stocks.py", viz_args)

        # 3. Generate Report
        # Logic for Rank vs Indiv
        file_name = os.path.basename(csv_path)
        # file_name example: stock_analysis_results_20260210.csv

        if args.code_list:
            # List specified -> Indiv
            html_output_dir = STOCKS_INDIV_DIR
            # User said: "コードリストから銘柄指定で生成されるhtmlファイルをreports/stocks/indivに保存する"
            # Filename? "stock_analysis_report_{date}.html" (or custom?)
            # Let's keep consistent naming.
            base_name = f"stock_analysis_report_{date_part}"
        else:
            # No list (Scan) -> Rank
            html_output_dir = STOCKS_RANK_DIR
            # User said: "htmlファイルの名前にrankを付けて,reports/stocks/rankに保存する"
            base_name = f"stock_analysis_report_rank_{date_part}"

        html_filename = f"{base_name}.html"
        html_path = os.path.join(html_output_dir, html_filename)

        # Relative path to charts
        # Charts are in ../charts from both rank and indiv folders
        chart_rel_path = "../charts"

        # Windows formatting for args? Python handles / fine usually.

        run_script(
            "src/generate_stock_report.py",
            [
                "--input-csv",
                csv_path,
                "--output-html",
                html_path,
                "--chart-rel-path",
                chart_rel_path,
                "--date",
                date_part,
            ],
        )

        print("=== All steps completed successfully ===")
        print(f"Report: {html_path}")

    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
