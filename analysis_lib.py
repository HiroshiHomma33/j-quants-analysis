import logging
import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# 親ディレクトリをパスに追加してjq_api_libsをインポート可能にする
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import config
from jq_api_libs import jq_api

# 定数定義
# src/analysis_lib.py -> get path from config
JQDB_PATH = config.get_jqdb_dir()

THRESHOLD_VOLUME_SURGE = 1.5
THRESHOLD_PRICE_FLAT = 0.002
THRESHOLD_TRADING_VALUE_RANK = 0.75


def get_fiscal_year_config():
    """
    現在の日付を基準に、前年度始、前年度末、今年度始の日付ウィンドウを決定する。
    加えて、6ヶ月前、3ヶ月前、1ヶ月前、前週末の日付ウィンドウも設定する。
    """
    now = datetime.now()

    # 基準日(Latest)の決定
    # 土日なら直前の金曜日を基準日とする
    if now.weekday() >= 5:  # Sat=5, Sun=6
        latest_date = now - timedelta(days=(now.weekday() - 4))
    else:
        latest_date = now

    curr_fy_start_year = (
        latest_date.year if latest_date.month >= 4 else latest_date.year - 1
    )

    # Fixed Dates
    prev_fy_start = datetime(curr_fy_start_year - 1, 4, 1)
    prev_fy_end = datetime(curr_fy_start_year, 3, 31)
    curr_fy_start = datetime(curr_fy_start_year, 4, 1)

    # Relative Dates
    # 前週末: 基準日の週の前週の金曜日 (Last Friday relative to the start of this week)
    days_since_monday = latest_date.weekday()
    monday_of_week = latest_date - timedelta(days=days_since_monday)
    prev_weekend = monday_of_week - timedelta(days=3)  # Mon - 3 days = Fri

    # DateOffset is pandas specific
    ts_latest = pd.Timestamp(latest_date)
    mo1_ago = ts_latest - pd.DateOffset(months=1)
    mo3_ago = ts_latest - pd.DateOffset(months=3)
    mo6_ago = ts_latest - pd.DateOffset(months=6)

    def date_to_str(d):
        return d.strftime("%Y-%m-%d")

    # Define windows (lookback 10 days to ensure we find a trading day)
    def make_window(target_date):
        # Handle pandas Timestamp or python datetime
        if isinstance(target_date, pd.Timestamp):
            target_date = target_date.to_pydatetime()
        return (date_to_str(target_date - timedelta(days=10)), date_to_str(target_date))

    config = {
        "prev_fy_start_window": (
            date_to_str(prev_fy_start),
            date_to_str(prev_fy_start + timedelta(days=10)),
        ),
        "prev_fy_end_window": (
            date_to_str(prev_fy_end - timedelta(days=10)),
            date_to_str(prev_fy_end),
        ),
        "curr_fy_start_window": (
            date_to_str(curr_fy_start),
            date_to_str(curr_fy_start + timedelta(days=10)),
        ),
        "latest_window": make_window(latest_date),
        "prev_weekend_window": make_window(prev_weekend),
        "1w_window": make_window(ts_latest - timedelta(days=7)),
        "1mo_window": make_window(mo1_ago),
        "3mo_window": make_window(mo3_ago),
        "6mo_window": make_window(mo6_ago),
    }

    logging.info(f"Analysis Date Config: {config}")
    return config


def get_topix_data(id_token, date_config):
    logging.info("Fetching TOPIX Data...")
    points = {}
    try:
        for key, window in date_config.items():
            df = jq_api.get_topix(id_token, from_date=window[0], to_date=window[1])
            if not df.empty:
                df = df.sort_values("Date")
                if "start" in key:
                    points[key] = df.iloc[0]
                else:
                    # For end/latest/time-series windows, take the last available date
                    points[key] = df.iloc[-1]
                    if key == "latest_window" and len(df) >= 2:
                        points["latest_prev"] = df.iloc[-2]
            else:
                logging.warning(f"TOPIX data empty for window {key}")

        metrics = {}

        def to_float(val):
            try:
                return float(val)
            except (ValueError, TypeError):
                return np.nan

        if "prev_fy_end_window" in points and "prev_fy_start_window" in points:
            end = to_float(points["prev_fy_end_window"]["Close"])
            start = to_float(points["prev_fy_start_window"]["Close"])
            if start > 0:
                metrics["PrevFY_Change"] = (end / start) - 1
            else:
                metrics["PrevFY_Change"] = 0.0

        # Base for YTD calculations
        if "curr_fy_start_window" in points:
            ytd_start_val = to_float(points["curr_fy_start_window"]["Close"])
        else:
            ytd_start_val = None

        if "latest_window" in points and ytd_start_val not in [None, 0]:
            latest = to_float(points["latest_window"]["Close"])
            metrics["YTD_Change"] = (latest / ytd_start_val) - 1
        else:
            metrics["YTD_Change"] = 0.0

        # Calculate YTD Change for other time points for comparison
        time_points = ["6mo", "3mo", "1mo", "prev_weekend"]
        for suffix in time_points:
            key = f"{suffix}_window"
            if key in points and ytd_start_val not in [None, 0]:
                val = to_float(points[key]["Close"])
                metrics[f"YTD_Change_{suffix}"] = (val / ytd_start_val) - 1
            else:
                metrics[f"YTD_Change_{suffix}"] = np.nan

        logging.info(f"TOPIX Metrics: {metrics}")

        # LatestDateを取得
        if "latest_window" in points:
            try:
                latest_date_val = points["latest_window"]["Date"]
                if isinstance(latest_date_val, pd.Timestamp):
                    metrics["LatestDate"] = latest_date_val.strftime("%Y%m%d")
                else:
                    metrics["LatestDate"] = pd.to_datetime(latest_date_val).strftime(
                        "%Y%m%d"
                    )
            except Exception as e:
                logging.warning(f"Could not extract date from TOPIX data: {e}")

        return metrics
    except Exception as e:
        logging.error(f"Error fetching TOPIX data: {e}")
        return {"PrevFY_Change": 0.0, "YTD_Change": 0.0}


def load_stock_data_from_local(code, sector_code):
    """
    ローカルのjqdbから指定された銘柄のデータを読み込む。
    対象：jqdb/daily_quotes/<sector_code>/<code>_*fy*.csv
    """
    sector_dir = JQDB_PATH / sector_code
    if not sector_dir.exists():
        logging.warning(f"Sector directory not found: {sector_dir}")
        return None

    # ファイル検索
    pattern = f"{code}_*fy*.csv"
    files = list(sector_dir.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No data files found for code {code} in {sector_dir} matching pattern {pattern}"
        )

    # 全ファイルを読み込んで結合
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            dfs.append(df)
        except Exception as e:
            # Add strict error handling as requested
            msg = f"Failed to read file {f}: {e}"
            logging.error(msg)
            raise ValueError(msg) from e

    if not dfs:
        raise ValueError(f"No valid dataframes loaded for code {code}")

    big_df = pd.concat(dfs, ignore_index=True)

    # 日付変換とソート
    if "Date" in big_df.columns:
        big_df["Date"] = pd.to_datetime(big_df["Date"])
        big_df = big_df.sort_values("Date")

    if big_df.empty:
        raise ValueError(f"Loaded data for code {code} is empty after concatenation.")

    return big_df


def fetch_stock_data_from_api(id_token, code, start_date="2021-04-01"):
    """
    APIから直接株価データを取得する (テスト用)
    """
    now = datetime.now()
    end_date = now.strftime("%Y-%m-%d")
    try:
        logging.info(f"Fetching data for {code} from API ({start_date}-{end_date})...")
        df = jq_api.get_daily_quotes(
            id_token, code=code, from_date=start_date, to_date=end_date
        )
        if df.empty:
            return None

        # 日付変換とソート
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values("Date")

        return df
    except Exception as e:
        logging.error(f"API fetch failed for {code}: {e}")
        return None


def extract_stock_points(df_stock, date_config):
    """
    1銘柄分のDataFrameから複数地点のPointを抽出する
    """
    points = {}

    def get_window_df(d_start, d_end):
        ts_start = pd.to_datetime(d_start)
        ts_end = pd.to_datetime(d_end)
        return df_stock[(df_stock["Date"] >= ts_start) & (df_stock["Date"] <= ts_end)]

    # 1. search for points
    # Define mapping for "Latest" type points (fetch last in window)
    window_keys = [
        "prev_fy_start_window",
        "prev_fy_end_window",
        "curr_fy_start_window",
        "latest_window",
        "prev_weekend_window",
        "1w_window",
        "1mo_window",
        "3mo_window",
        "6mo_window",
    ]

    for k in window_keys:
        if k in date_config:
            w = date_config[k]
            sub = get_window_df(w[0], w[1])
            if not sub.empty:
                points[k] = sub.iloc[-1]

                # Fetch prev day data for Volume Change calc
                target_date = sub.iloc[-1]["Date"]
                prev_data = df_stock[df_stock["Date"] < target_date]
                if not prev_data.empty:
                    points[f"{k}_prev_day"] = prev_data.iloc[-1]

            if not sub.empty:
                points[k] = sub.iloc[0]

    # Calculate Trend
    # Need last 75 days at least
    if not df_stock.empty and len(df_stock) >= 75:
        # Sort just in case
        # df_stock is already sorted in fetch/load functions

        # We only need the latest values, but rolling requires series
        # To avoid performance hit on large DF, take last 100 rows
        df_recent = df_stock.iloc[-100:].copy()

        df_recent["MA25"] = df_recent["Close"].rolling(window=25).mean()
        df_recent["MA75"] = df_recent["Close"].rolling(window=75).mean()

        latest_row = df_recent.iloc[-1]
        price = latest_row["Close"]
        ma25 = latest_row["MA25"]
        ma75 = latest_row["MA75"]

        status = "Neutral"
        if not np.isnan(ma25) and not np.isnan(ma75):
            if price > ma25 > ma75:
                status = "Uptrend"
            elif price < ma25 < ma75:
                status = "Downtrend"
            elif ma25 > ma75:
                status = "Neutral (Bullish Bias)"
            else:
                status = "Neutral (Bearish Bias)"

        points["Trend_Status"] = status

    return points


def calculate_stock_metrics(points):
    metrics = {}

    def to_float(val):
        try:
            return float(val)
        except (ValueError, TypeError):
            return np.nan

    # 1. Prev FY Change
    p_start = points.get("prev_fy_start_window")
    p_end = points.get("prev_fy_end_window")

    if p_start is not None and p_end is not None:
        end = to_float(p_end["Close"])
        start = to_float(p_start["Close"])
        if start > 0:
            metrics["PrevFY_Change"] = (end / start) - 1
        else:
            metrics["PrevFY_Change"] = np.nan
    else:
        metrics["PrevFY_Change"] = np.nan

    # 2. Main YTD Change (Current FY Start vs Latest)
    c_start = points.get("curr_fy_start_window")
    latest = points.get("latest_window")

    if c_start is not None and latest is not None:
        lat_val = to_float(latest["Close"])
        start_val = to_float(c_start["Close"])
        if start_val > 0:
            metrics["YTD_Change"] = (lat_val / start_val) - 1
        else:
            metrics["YTD_Change"] = np.nan
    else:
        metrics["YTD_Change"] = np.nan

    # 3. Latest Basic Info
    if latest is not None:
        latest_close = to_float(latest["Close"])
        latest_vol = to_float(latest["Volume"])
        metrics["Close"] = latest_close
        metrics["TradingValue"] = latest_close * latest_vol

        # Prev Day for Latest
        latest_prev = points.get("latest_window_prev_day")

        if latest_prev is not None:
            prev_close = to_float(latest_prev["Close"])
            prev_vol = to_float(latest_prev["Volume"])

            if prev_close > 0:
                metrics["Latest_PriceChange"] = (latest_close / prev_close) - 1
            else:
                metrics["Latest_PriceChange"] = np.nan

            if prev_vol > 0:
                metrics["Latest_VolumeChange"] = (latest_vol / prev_vol) - 1
            else:
                metrics["Latest_VolumeChange"] = np.nan
        else:
            metrics["Latest_PriceChange"] = np.nan
            metrics["Latest_VolumeChange"] = np.nan
    else:
        metrics["Close"] = np.nan
        metrics["TradingValue"] = 0
        metrics["Latest_PriceChange"] = np.nan
        metrics["Latest_VolumeChange"] = np.nan

    # 4. Relative Time Series Metrics
    key_map = {
        "6mo": "6mo_window",
        "3mo": "3mo_window",
        "1mo": "1mo_window",
        "1w": "1w_window",
        "PrevWeekend": "prev_weekend_window",
    }

    for suffix, window_key in key_map.items():
        pt = points.get(window_key)

        # YTD Return (Cumulative from FY Start)
        col_ytd = f"YTD_Change_{suffix}"
        if pt is not None and c_start is not None:
            val = to_float(pt["Close"])
            start_val = to_float(c_start["Close"])
            if start_val > 0:
                metrics[col_ytd] = (val / start_val) - 1
            else:
                metrics[col_ytd] = np.nan
        else:
            metrics[col_ytd] = np.nan

        # Volume Change (At that point)
        col_vol = f"VolChange_{suffix}"
        pt_prev = points.get(f"{window_key}_prev_day")

        if pt is not None and pt_prev is not None:
            v_now = to_float(pt["Volume"])
            v_prev = to_float(pt_prev["Volume"])
            if v_prev > 0:
                metrics[col_vol] = (v_now / v_prev) - 1
            else:
                metrics[col_vol] = np.nan
        else:
            metrics[col_vol] = np.nan

        # Period Return (Return over last X months: Latest / Point_Ago - 1)
        # Note: "PrevWeekend" is treated as specific point return too
        col_ret = f"Return_{suffix}"
        if pt is not None and latest is not None:
            val_now = to_float(latest["Close"])
            val_past = to_float(pt["Close"])
            if val_past > 0:
                metrics[col_ret] = (val_now / val_past) - 1
            else:
                metrics[col_ret] = np.nan
        else:
            metrics[col_ret] = np.nan

    # 5. Trend Analysis (MA25, MA75)
    # This requires time-series data which `points` doesn't fully have.
    # We need to calculate this BEFORE extracting points or pass the full DF?
    # `extract_stock_points` takes `df_stock`.
    # But `calculate_stock_metrics` only takes `points`.
    # To avoid changing signature too much, we should calculate MAs in `extract_stock_points`
    # and pass them as "Latest_MA25", "Latest_MA75" in points?
    # OR, calculate Trend inside `extract_stock_points` and pass it.

    # We will assume "Trend_Status" is passed in `points` if available.
    if "Trend_Status" in points:
        metrics["Trend_Status"] = points["Trend_Status"]
    else:
        metrics["Trend_Status"] = "Unknown"

    return metrics


def assign_supply_demand_signal(df_results):
    if df_results.empty:
        return df_results
    threshold_val = df_results["TradingValue"].quantile(THRESHOLD_TRADING_VALUE_RANK)
    signals = []
    for _, row in df_results.iterrows():
        p = row["Latest_PriceChange"]
        v = row["Latest_VolumeChange"]
        t = row["TradingValue"]

        sig = "-"
        if p > 0 and v >= THRESHOLD_VOLUME_SURGE and t >= threshold_val:
            sig = "強い買い"
        elif p > 0 and v <= 0:
            sig = "信頼度低の上昇"
        elif abs(p) < THRESHOLD_PRICE_FLAT and v >= THRESHOLD_VOLUME_SURGE:
            sig = "転換点のサイン"
        signals.append(sig)
    df_results["Status_Signal"] = signals
    return df_results


def get_latest_sector_csv():
    """
    最新のセクター分析結果CSVのパスを返す
    """
    sector_dir = config.get_sector_data_dir()
    if not sector_dir.exists():
        return None

    # Pattern: sector_analysis_results_YYYYMMDD.csv
    files = list(sector_dir.glob("sector_analysis_results_*.csv"))
    if not files:
        return None

    # Sort by name (date is in filename YYYYMMDD so alphabetic sort works)
    files.sort()
    return files[-1]


def load_topix_timeseries(id_token=None, date_config=None):
    """
    TOPIXの時系列データを読み込み、DataFrameとして返す。
    jqdb/indices/topix_daily.csv を優先し、存在しなければAPIから取得を試みるが、
    基本は indices ディレクトリにあることを前提とする。
    """
    topix_path = config.get_topix_file_path()

    if os.path.exists(topix_path):
        try:
            df = pd.read_csv(topix_path)
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
                df = df.sort_values("Date")
            return df
        except Exception as e:
            logging.error(f"Failed to read TOPIX file {topix_path}: {e}")

    # Fallback to API if id_token and date_config are provided (usually they are not for this simple load)
    if id_token and date_config:
        # Re-use logic from get_topix_data but returning DF
        # This is complex because get_topix_data stitches windows.
        # For a chart we need a continuous timeline.
        # Let's assume we need to fetch 'from 6 months ago'.
        start_date = date_config["6mo_window"][0]
        end_date = date_config["latest_window"][1]
        try:
            df = jq_api.get_topix(id_token, from_date=start_date, to_date=end_date)
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
                df = df.sort_values("Date")
            return df
        except Exception as e:
            logging.error(f"Failed to fetch TOPIX from API: {e}")

    return pd.DataFrame()  # Empty if failed


def get_sector_index(sector_code):
    """
    指定されたセクターの「合成指数」を算出する。

    ロジック:
    1. jqdb/daily_quotes/{sector_code} 以下の全CSVを読み込む。
    2. 各銘柄について、日次リターン (PctChange) を計算する。
    3. 同一日付における全銘柄のリターンの平均値 (Equal Weight) を計算する。
       ※ 時価総額加重平均は、時価総額データが時系列で揃っていないため困難。ここでは等加重とする。
    4. 基準日（データ存在する最古の日付）を100として累積リターン指数を作成する。

    Returns:
        pd.DataFrame: columns=['Date', 'Close'] (Closeは指数値)
    """
    sector_dir = JQDB_PATH / sector_code
    if not sector_dir.exists():
        logging.warning(f"Sector directory not found for index calc: {sector_dir}")
        return pd.DataFrame()

    files = list(sector_dir.glob("*.csv"))
    if not files:
        return pd.DataFrame()

    # 多すぎる場合はサンプリング？いや、精度のため全件読むべきだが、パフォーマンス注意。
    # Test modeなどは呼び出し元で制御されるべきだが、ここはライブラリなので全件読む。

    all_returns = []

    for f in files:
        try:
            # 必要なのは Date と Close のみ
            df = pd.read_csv(f, usecols=["Date", "Close"])
            if df.empty:
                continue

            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values("Date")

            # 日次リターン計算
            df["Return"] = df["Close"].pct_change()

            df = df.dropna(subset=["Return"])
            df = df[["Date", "Return"]]

            all_returns.append(df)

        except Exception as e:
            logging.debug(f"Skipping file {f.name} in sector index calc: {e}")
            continue

    if not all_returns:
        return pd.DataFrame()

    # 結合して日付ごとの平均を出す
    # concat して groupby('Date').mean() が速い
    big_df = pd.concat(all_returns)
    daily_avg_return = big_df.groupby("Date")["Return"].mean().sort_index()

    if daily_avg_return.empty:
        return pd.DataFrame()

    # 指数化 (100スタート)
    # (1 + r1) * (1 + r2) ...
    cumulative_returns = (1 + daily_avg_return).cumprod()

    # 最初の日の前日を1.0と仮定したいが、データがないので
    # 初日を 1.0 * (1+r) から始めるか、あるいは
    # 累積リターン系列をそのまま価格として扱う。
    # チャート描画時に正規化されるので、絶対値は重要ではない。
    # Close = cumulative_returns * 100

    sector_index = pd.DataFrame(
        {"Date": daily_avg_return.index, "Close": cumulative_returns * 100}
    ).reset_index(drop=True)

    return sector_index
