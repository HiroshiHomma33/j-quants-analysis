import os
import subprocess
import sys

import pandas as pd

from src import config


def run_script_capture(script_path, script_args=None):
    """
    指定されたPythonスクリプトを実行し、標準出力をキャプチャして返す。
    エラーが発生した場合はNoneを返し、エラー内容を表示する。
    """
    cmd = [sys.executable, script_path]
    if script_args:
        cmd.extend(script_args)

    print(f"--- Running {script_path} with args: {script_args} ---")
    try:
        # capture_output=True で標準出力を取得 (text=True で文字列として)
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(result.stdout)  # 実行ログとして表示
        print(f"--- {script_path} finished successfully ---\n")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_path}: {e}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return None


def parse_key_value_output(stdout_text):
    """
    標準出力から KEY=VALUE 形式の行を抽出して辞書にする
    """
    data = {}
    if not stdout_text:
        return data

    for line in stdout_text.splitlines():
        if "=" in line:
            parts = line.split("=", 1)
            key = parts[0].strip()
            val = parts[1].strip()
            data[key] = val
    return data


def main():
    # Ensure directories exist via config
    config.ensure_directories()

    # 成果物の保存先ディレクトリ (from config)
    # Sector Report HTML goes to sector dir
    SECTOR_REPORTS_DIR = str(config.get_sector_dir())
    SECTOR_DATA_DIR = str(config.get_sector_data_dir())
    SECTOR_CHARTS_DIR = str(config.get_sector_charts_dir())

    # 1. データ収集・分析 (src/industry_analysis.py)
    # 出力先ディレクトリを渡す (CSV data)
    analysis_script = "src/industry_analysis.py"
    analysis_args = ["--output-dir", SECTOR_DATA_DIR]

    # 既存の引数があれば渡す（例: --test）
    if len(sys.argv) > 1:
        analysis_args.extend(sys.argv[1:])

    stdout = run_script_capture(analysis_script, analysis_args)
    if stdout is None:
        print("Aborting: Analysis step failed.")
        sys.exit(1)

    # 出力からファイルパスと日付情報を取得
    parsed_data = parse_key_value_output(stdout)
    csv_path = parsed_data.get("OUTPUT_CSV")
    data_date = parsed_data.get("DATA_DATE")

    if not csv_path or not os.path.exists(csv_path):
        # テストモードなどでCSVが出力されなかった場合
        print(
            "Analysis finished but no CSV output detected (or Test mode). Stopping pipeline."
        )
        return

    print(f"Pipeline Info -> Date: {data_date}, CSV: {csv_path}")

    # --- Market Context Analysis Integration ---
    try:
        from src import visualize_context
        from src.market_context import MarketContextAnalyzer

        print("--- Running Market Context Analysis ---")
        analyzer = MarketContextAnalyzer()
        analyzer.load_data()

        # Load the sector analysis results we just generated
        df_sector = pd.read_csv(csv_path)

        # Calculate Context Metrics (RS, Trends)
        df_context = analyzer.calculate_metrics(df_sector)

        if df_context is not None:
            # Overwrite the CSV with enhanced data (including RS, Trend)
            df_context.to_csv(csv_path, index=False)
            print(f"Enhanced Sector Data saved to {csv_path}")

            # Generate Context Charts
            rotation_png = os.path.join(
                SECTOR_CHARTS_DIR, f"sector_rotation_{data_date}.png"
            )
            heatmap_png = os.path.join(
                SECTOR_CHARTS_DIR, f"sector_heatmap_{data_date}.png"
            )

            visualize_context.plot_sector_rotation(df_context, rotation_png)
            visualize_context.plot_sector_heatmap(df_context, heatmap_png)

            print(f"Context Charts Generated: {rotation_png}, {heatmap_png}")
        else:
            print("Warning: Context analysis returned None.")

    except Exception as e:
        print(f"Error in Market Context Analysis: {e}")
        # Don't stop pipeline, proceed with what we have
    # -------------------------------------------

    # ファイル名の定義 (日付ベース)
    png_filename = f"sector_returns_{data_date}.png"
    png_timeline_filename = f"sector_returns_timeline_{data_date}.png"
    rotation_filename = f"sector_rotation_{data_date}.png"
    heatmap_filename = f"sector_heatmap_{data_date}.png"
    html_filename = f"industry_report_{data_date}.html"

    # Paths constructed using config directories
    png_path = os.path.join(SECTOR_CHARTS_DIR, png_filename)
    png_timeline_path = os.path.join(SECTOR_CHARTS_DIR, png_timeline_filename)
    html_path = os.path.join(SECTOR_REPORTS_DIR, html_filename)

    # 2. グラフ作成 (src/visualize_results.py)
    # Note: visualize_results.py generates the basic bar charts
    viz_script = "src/visualize_results.py"
    viz_args = [
        "--input-csv",
        csv_path,
        "--output-png",
        png_path,
        "--output-timeline-png",
        png_timeline_path,
    ]

    if run_script_capture(viz_script, viz_args) is None:
        print("Aborting: Visualization step failed.")
        sys.exit(1)

    # 3. HTMLレポート生成 (src/generate_report.py)
    # HTMLから画像への相対パス
    # HTML is in market_analysis/reports/sector/
    # Charts are in market_analysis/reports/sector/charts/
    # Relative path: charts/filename.png
    image_rel_path = os.path.join("charts", png_filename).replace("\\", "/")
    timeline_image_rel_path = os.path.join("charts", png_timeline_filename).replace(
        "\\", "/"
    )
    rotation_image_rel_path = os.path.join("charts", rotation_filename).replace(
        "\\", "/"
    )
    heatmap_image_rel_path = os.path.join("charts", heatmap_filename).replace("\\", "/")

    report_script = "src/generate_report.py"
    report_args = [
        "--input-csv",
        csv_path,
        "--output-html",
        html_path,
        "--image-path",
        image_rel_path,
        "--timeline-image-path",
        timeline_image_rel_path,
        "--rotation-image-path",
        rotation_image_rel_path,
        "--heatmap-image-path",
        heatmap_image_rel_path,
    ]

    if run_script_capture(report_script, report_args) is None:
        print("Aborting: Report generation step failed.")
        sys.exit(1)

    print("=== All analysis steps completed successfully ===")
    print(f"Report: {os.path.abspath(html_path)}")


if __name__ == "__main__":
    main()
