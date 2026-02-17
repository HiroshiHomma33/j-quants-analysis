import logging
import os
from datetime import timedelta

import numpy as np
import pandas as pd

from . import config

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class MarketContextAnalyzer:
    def __init__(self):
        self.indices_dir = config.get_indices_dir()
        self.topix_file = config.get_topix_file_path()
        self.sector_dir = config.get_sector_data_dir()
        self.daily_quotes_dir = config.get_jqdb_dir()

        self.df_topix = None
        self.df_sectors = {}  # Key: Sector Code, Value: DataFrame
        self.context_data = None  # DataFrame storing calculation results

    def load_data(self):
        """
        TOPIXデータとセクターデータをロードする
        """
        # Load TOPIX
        if os.path.exists(self.topix_file):
            try:
                self.df_topix = pd.read_csv(self.topix_file)
                if self.df_topix.empty:
                    raise ValueError(f"TOPIX file is empty: {self.topix_file}")
                self.df_topix["Date"] = pd.to_datetime(self.df_topix["Date"])
                self.df_topix = self.df_topix.sort_values("Date")
            except Exception as e:
                logging.error(f"Failed to load TOPIX data: {e}")
                raise ValueError(
                    f"Failed to load TOPIX data from {self.topix_file}: {e}"
                ) from e
        else:
            msg = f"TOPIX file not found: {self.topix_file}"
            logging.error(msg)
            raise FileNotFoundError(msg)

        # Load Sector Data (Expected to be in sector/data/sector_analysis_results_*.csv or similar?)
        # Implementaion Plan says: jqdb/daily_quotes/0050/ etc. as generic sector approach?
        # OR using aggregated sector returns?
        # "全セクターデータ ... jqdb/daily_quotes/0050/ 等のセクターCSV"
        # The user request mentioned "3.2. ... jqdb/daily_quotes/0050/ 等のセクターCSV".
        # However, `run_industry_analysis.py` generates `sector_analysis_results_YYYYMMDD.csv`.
        # Using processed sector data might be easier, but raw data allows custom timeframe calculations.
        # Let's try to load individual sector indices from `jqdb/daily_quotes` if they exist as "indices"?
        # Wait, 33 sector indices are treated as stocks in some contexts or have specific codes?
        # User said "jqdb/daily_quotes/0050/". 0050 is "Fishery, Agriculture & Forestry".
        # It seems sector indices are stored like stocks in `jqdb/daily_quotes/{sector_code}/*`.
        # Check `jqdb/daily_quotes/0050` existence later.
        pass

    def _calculate_trend(self, df):
        """
        MA25, MA75に基づいてトレンドを判定する
        """
        if df is None or df.empty or len(df) < 75:
            return "Unknown"

        df = df.copy()
        df["MA25"] = df["Close"].rolling(window=25).mean()
        df["MA75"] = df["Close"].rolling(window=75).mean()

        latest = df.iloc[-1]
        price = latest["Close"]
        ma25 = latest["MA25"]
        ma75 = latest["MA75"]

        if price > ma25 > ma75:
            return "Uptrend"
        elif price < ma25 < ma75:
            return "Downtrend"
        elif ma25 > ma75:
            return "Neutral (Bullish Bias)"  # MA25 > MA75 but Price dropped
        else:
            return "Neutral (Bearish Bias)"

    def calculate_metrics(self, df_sector_summary=None):
        """
        各種指標を計算する。
        df_sector_summary: industry_analysis.py で生成されたDataFrame（あれば利用）
        なければ独自に計算。
        ここでは、run_industry_analysis.py との統合を考え、
        industry_analysis.py が生成する `sector_analysis_results` をベースに
        TOPIXとの比較 (RS) を追加計算するアプローチを採る。
        """
        if self.df_topix is None or self.df_topix.empty:
            logging.warning("TOPIX data missing. Cannot calculate relative metrics.")
            return df_sector_summary

        # Normalize TOPIX
        # Calculate TOPIX returns for comparison
        # 1W, 1M, 3M, 6M
        # TOPIX Returns
        latest_date = self.df_topix["Date"].max()

        def get_pct_change(days):
            target_date = latest_date - timedelta(days=days)
            # Find closest date
            # simple lookup
            past_data = self.df_topix[self.df_topix["Date"] <= target_date]
            if past_data.empty:
                return 0.0
            start_price = past_data.iloc[-1]["Close"]
            end_price = self.df_topix.iloc[-1]["Close"]
            return (end_price - start_price) / start_price

        topix_ret_1w = get_pct_change(7)
        topix_ret_1m = get_pct_change(30)
        topix_ret_3m = get_pct_change(90)
        topix_ret_6m = get_pct_change(180)

        market_trend = self._calculate_trend(self.df_topix)

        # Update Sector Summary if provided
        # Update Sector Summary if provided
        if df_sector_summary is not None:
            df = df_sector_summary.copy()

            # Calculate RS (Relative Strength)
            # RS = Sector Return - TOPIX Return

            # Map of columns to TOPIX returns
            periods = {
                "1w": topix_ret_1w,
                "1mo": topix_ret_1m,
                "3mo": topix_ret_3m,
                "6mo": topix_ret_6m,
            }

            for period, topix_r in periods.items():
                col_ret = f"Return_{period}"
                col_rs = f"RS_{period}"

                if col_ret in df.columns:
                    df[col_rs] = df[col_ret] - topix_r
                else:
                    df[col_rs] = np.nan

            df["Market_Trend"] = market_trend

            return df

        return None

        return None

    def get_market_status(self):
        """
        現在の市場環境を返すシンプルメソッド
        """
        if self.df_topix is None:
            self.load_data()

        return {
            "Trend": self._calculate_trend(self.df_topix),
            "Latest_Date": self.df_topix["Date"].max().strftime("%Y-%m-%d")
            if self.df_topix is not None and not self.df_topix.empty
            else "N/A",
        }
