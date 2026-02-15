import argparse
import datetime
import logging
import os
import sys
import time

import pandas as pd

# VSCodeのワークスペースルートに jq_api_libs があると仮定してパスを通す
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from jq_api_libs import jq_api
except ImportError:
    print("エラー: jq_api_libs モジュールが見つかりません。")
    print("ディレクトリ構成が正しいか確認してください。")
    sys.exit(1)

# 設定
ROOT_PATH = r"C:\Users\hhomm\V_00\jqdb"
DAILY_QUOTES_DIR_NAME = "daily_quotes"
LISTED_INFO_DIR_NAME = "listed_info"
LOG_DIR_NAME = "log"

# タイムゾーン定義 (JST)
JST = datetime.timezone(datetime.timedelta(hours=9))


def get_latest_business_day(api_key):
    """
    取引カレンダーから直近の営業日を取得する。
    現在日付から過去へ遡って検索する。
    """
    # JSTで現在時刻を取得
    now = datetime.datetime.now(JST)
    to_date = now.strftime("%Y%m%d")

    # 18:30以前は当日データが不完全なため、前日までのカレンダーを取得対象とする
    if now.time() < datetime.time(18, 30):
        target_date = now - datetime.timedelta(days=1)
        to_date = target_date.strftime("%Y%m%d")
        logging.info(
            f"Time is before 18:30 JST ({now.time()}). Target end date set to {to_date}."
        )
    else:
        logging.info(
            f"Time is after 18:30 JST ({now.time()}). Target end date set to {to_date} (Today)."
        )

    # 2週間も遡れば十分休日明けでも営業日があるはず
    from_date = (now - datetime.timedelta(days=14)).strftime("%Y%m%d")

    try:
        # holiday_division="1" (平日) は指定できないAPI仕様かもしれないので、
        # 全取得してフィルタリングする
        df_calendar = jq_api.get_trading_calendar(
            api_key=api_key, from_date=from_date, to_date=to_date
        )

        if df_calendar.empty:
            raise ValueError("Trading calendar is empty.")

        # 休場区分(HolidayDivision)が '1' (営業日) のもの
        if "HolidayDivision" in df_calendar.columns:
            # 営業日のみ抽出 (API仕様: 1=営業日 0=非営業日)
            df_business_days = df_calendar[
                df_calendar["HolidayDivision"].astype(str) == "1"
            ]
        else:
            # カラムがない場合（仕様変更等）、とりあえず全データを候補にする
            df_business_days = df_calendar

        if df_business_days.empty:
            raise ValueError("No business days found in the last 2 weeks.")

        # 日付で降順ソート
        df_business_days = df_business_days.sort_values("Date", ascending=False)
        latest_date_str = df_business_days.iloc[0]["Date"]

        # YYYYMMDD形式に正規化 (APIがYYYY-MM-DDで返す場合に対応)
        return latest_date_str.replace("-", "")

    except Exception as e:
        logging.warning(f"Error fetching trading calendar: {e}")
        # フォールバック: 平日判定のみで返す（祝日考慮もれのリスクあり）
        logging.warning("Fallback to simple weekday check.")
        d = now
        while d.weekday() >= 5:  # 5=Sat, 6=Sun
            d -= datetime.timedelta(days=1)
        return d.strftime("%Y%m%d")


def get_paths(root_path):
    daily_quotes_path = os.path.join(root_path, DAILY_QUOTES_DIR_NAME)
    listed_info_path = os.path.join(root_path, LISTED_INFO_DIR_NAME)
    return daily_quotes_path, listed_info_path


def get_current_fy(date_obj: datetime.datetime | datetime.date) -> int:
    """
    日付から日本の会計年度を返す
    """
    if date_obj.month >= 4:
        return date_obj.year
    else:
        return date_obj.year - 1


class FileRepository:
    """
    全ファイルを事前スキャンし、メモリ上で管理するクラス。
    高速化のため、ループ内での glob.glob を廃止する。
    """

    def __init__(self, root_dir: str):
        self.root_dir = root_dir
        self.file_map: dict[str, list[str]] = {}
        self._scan_files()

    def _scan_files(self):
        logging.info(f"Scanning files in {self.root_dir}...")
        start_time = time.time()
        for root, _, files in os.walk(self.root_dir):
            for filename in files:
                if filename.endswith(".csv"):
                    # 想定ファイル名: {code}_{fy}fy_{yyyymmdd}.csv または {code}_{fy}fy.csv
                    # 例: 72030_2025fy_20260124.csv
                    parts = filename.split("_")
                    if len(parts) >= 2:
                        code = parts[0]
                        full_path = os.path.join(root, filename)
                        if code not in self.file_map:
                            self.file_map[code] = []
                        self.file_map[code].append(full_path)

        elapsed = time.time() - start_time
        elapsed = time.time() - start_time
        logging.info(
            f"Scanned {sum(len(v) for v in self.file_map.values())} files in {elapsed:.2f} seconds."
        )

    def get_latest_file(self, code: str) -> str | None:
        """
        指定されたコードの最新ファイルを返す。
        日付付きファイル優先、日付が大きいもの優先。
        """
        files = self.file_map.get(code)
        if not files:
            return None

        # ソートロジック:
        # ファイル名末尾の日付部分でソートしたい。
        # 72030_2024fy_20250320.csv -> key: 20250320
        # 72030_2024fy.csv -> key: 0 (アーカイブ済み等は日付なしとみなす、あるいは古いとみなす)

        def sort_key(filepath):
            filename = os.path.basename(filepath)
            parts = filename.split("_")
            last_part = parts[-1].split(".")[0]
            if last_part.isdigit() and len(last_part) == 8:
                return int(last_part)
            return 0  # 日付がない場合は優先度低

        # 日付順で降順ソート
        sorted_files = sorted(files, key=sort_key, reverse=True)
        return sorted_files[0] if sorted_files else None

    def add_file(self, code: str, filepath: str):
        """
        新規作成されたファイルを管理対象に追加（簡易的な同期）
        """
        if code not in self.file_map:
            self.file_map[code] = []
        if filepath not in self.file_map[code]:
            self.file_map[code].append(filepath)

    def remove_file(self, code: str, filepath: str):
        """
        削除されたファイルを管理対象から除外
        """
        if code in self.file_map:
            if filepath in self.file_map[code]:
                self.file_map[code].remove(filepath)


def atomic_save(df: pd.DataFrame, filepath: str):
    """
    一時ファイルを経由して安全に保存する (Atomic Write)。
    """
    temp_path = filepath + ".tmp"
    try:
        df.to_csv(temp_path, index=False, encoding="utf-8-sig")

        # Windowsにおいて os.replace はAtomicであるが、
        # 既存ファイルが開かれている場合などにPermissionErrorになることがある。
        # ここでは標準的な os.replace を使用する。
        if os.path.exists(filepath):
            os.replace(
                temp_path, filepath
            )  # Unix: rename, Windows: replace (Python 3.3+)
        else:
            os.rename(temp_path, filepath)

    except Exception as e:
        logging.error(f"    Error in atomic_save: {e}")
        # クリーンアップ
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise e


def update_file_for_code(
    api_key,
    code,
    sector_code,
    daily_quotes_dir,
    latest_biz_date,
    test_mode,
    wait_time,
    file_repo: FileRepository,
):
    """
    1銘柄分のファイルをアップデートする。
    FileRepositoryを使用し、年度またぎ（Rollover）に対応。
    """

    # 該当セクターディレクトリ (保存用)
    sector_dir = os.path.join(daily_quotes_dir, sector_code)
    if not os.path.exists(sector_dir):
        # セクターディレクトリがない場合はスキップ（build_jqdb.py未実行とみなす）
        return "DIR_NOT_FOUND"

    # レポジトリから最新ファイル取得
    current_file_path = file_repo.get_latest_file(code)

    last_updated_date_str = None
    df_old = pd.DataFrame()

    if current_file_path:
        filename = os.path.basename(current_file_path)
        try:
            # ファイル名形式: {code}_{fy}fy_{date}.csv
            parts = filename.split("_")
            if len(parts) >= 3:
                last_updated_date_str = parts[-1].split(".")[0]
            else:
                # アーカイブ済みファイル等の場合 (日付なし)
                # ファイルの中身を見て最終日を確認する必要があるが、
                # 今回の仕様ではファイル名に日付があるものを「最新」として扱っている前提。
                # 日付がないファイルしか見つからない＝アーカイブ済み＝新年度データがない状態と推測。
                # その場合は、そのファイルの最終日を取得するか、適当な古い日付を設定するか。
                # ここでは「ファイル読み込み」コストを避けるため、アーカイブファイル名からは取得不可として扱う。
                # ただし、ロジック上「続き」を取得したいので、ファイルを開いて最終日を確認する。
                try:
                    df_temp = pd.read_csv(current_file_path, dtype={"Date": str})
                    if not df_temp.empty and "Date" in df_temp.columns:
                        last_updated_date_str = df_temp["Date"].max().replace("-", "")
                except Exception:
                    pass
        except Exception:
            return "FILENAME_FORMAT_ERROR"

    if not last_updated_date_str:
        # ファイルがない、または日付が特定できない -> 新規扱いだが、
        # ディスクI/O削減の観点と「既存ファイル更新」の目的から、
        # ここでは「FILE_NOT_FOUND」としてスキップ（または必要なら全期間取得）
        # 要件「プログラム起動直後に...スキャン」によりファイルがない場合はここでわかる
        return "FILE_NOT_FOUND"

    if last_updated_date_str >= latest_biz_date:
        return "ALREADY_LATEST"

    # アップデートが必要
    last_dt = datetime.datetime.strptime(last_updated_date_str, "%Y%m%d")
    from_dt = last_dt + datetime.timedelta(days=1)
    from_date_str = from_dt.strftime("%Y%m%d")

    if from_date_str > latest_biz_date:
        return "DATE_CALC_ERROR"

    logging.info(f"  Fetching update for {code}: {from_date_str} - {latest_biz_date}")

    try:
        if test_mode:
            logging.info(
                f"  [TEST] Would fetch {from_date_str}-{latest_biz_date} and update."
            )
            return "UPDATED"

        # レート制限対策
        time.sleep(wait_time)

        # APIから不足分を取得
        df_new = jq_api.get_daily_quotes(
            api_key=api_key,
            code=code,
            from_date=from_date_str,
            to_date=latest_biz_date,
        )

        if df_new is None or df_new.empty:
            logging.info(f"    No new data fetched for {code}.")
            return "NO_NEW_DATA"

        logging.info(f"    Fetched {len(df_new)} rows.")

        # --- Rollover Logic Check ---
        # 取得データの中に4月1日が含まれているか判定
        # df_new['Date'] は YYYY-MM-DD 文字列
        # 年度替わり: 3/31 と 4/1 の境界

        # 日付ソート
        df_new = df_new.sort_values("Date")

        # 既存データをロード
        df_old = pd.read_csv(current_file_path, dtype={"Date": str})

        # 最新のデータの日付から年度を判定（API取得データの最新）
        latest_data_date_str = df_new.iloc[-1]["Date"]  # YYYY-MM-DD
        latest_data_date = datetime.datetime.strptime(latest_data_date_str, "%Y-%m-%d")
        new_fy = get_current_fy(latest_data_date)

        # 現在のファイルの年度（ファイル名から推定）
        # current_file_path: .../72030_2024fy_20250320.csv
        current_filename = os.path.basename(current_file_path)
        current_fy_str = current_filename.split("_")[1].replace("fy", "")  # 2024
        try:
            current_file_fy = int(current_fy_str)
        except ValueError:
            # ファイル名から年度が取れない場合、現在日付から推測するか、デフォルト
            current_file_fy = new_fy  # 同一とみなす

        if new_fy > current_file_fy:
            # 年度またぎ発生 (例: 2024fy -> 2025fy)
            logging.info(
                f"    [Rollover] Detected fiscal year change: {current_file_fy} -> {new_fy}"
            )

            # 1. データを分割
            # 新年度の開始日 (例: 2025-04-01)
            new_fy_start_date = datetime.datetime(new_fy, 4, 1).strftime("%Y-%m-%d")

            df_curr_fy_new_data = df_new[df_new["Date"] < new_fy_start_date]
            df_next_fy_new_data = df_new[df_new["Date"] >= new_fy_start_date]

            # 2. 旧年度の確定 (Archiving)
            if not df_curr_fy_new_data.empty:
                df_old_updated = pd.concat(
                    [df_old, df_curr_fy_new_data], ignore_index=True
                )
                df_old_updated = df_old_updated.drop_duplicates(
                    subset=["Date"], keep="last"
                ).sort_values("Date")
            else:
                df_old_updated = df_old

            # アーカイブファイル名: 72030_2024fy.csv (日付サフィックスなし)
            archive_filename = f"{code}_{current_file_fy}fy.csv"
            archive_path = os.path.join(sector_dir, archive_filename)

            logging.info(f"    Archiving old FY data to: {archive_filename}")
            atomic_save(df_old_updated, archive_path)

            # 元のファイル(日付付き)は削除する（アーカイブ名にリネームした扱い）
            if current_file_path != archive_path:
                try:
                    os.remove(current_file_path)
                    file_repo.remove_file(code, current_file_path)  # レポジトリ同期
                except OSError as e:
                    logging.warning(
                        f"    Warning: Failed to delete old file {current_file_path}: {e}"
                    )

            file_repo.add_file(code, archive_path)  # レポジトリ同期

            # 3. 新年度の開始
            if not df_next_fy_new_data.empty:
                # 日付フォーマット
                df_next_fy_new_data["Date"] = pd.to_datetime(
                    df_next_fy_new_data["Date"]
                ).dt.strftime("%Y-%m-%d")

                # 新ファイル名: 72030_2025fy_{latest_date}.csv
                # latest_data_date_str は YYYY-MM-DD なので YYYYMMDD に変換
                latest_date_plain = latest_data_date_str.replace("-", "")
                new_filename = f"{code}_{new_fy}fy_{latest_date_plain}.csv"
                new_file_path = os.path.join(sector_dir, new_filename)

                logging.info(f"    Creating new FY file: {new_filename}")
                atomic_save(df_next_fy_new_data, new_file_path)
                file_repo.add_file(code, new_file_path)

        else:
            # 通常更新 (年度内)
            df_combined = pd.concat([df_old, df_new], ignore_index=True)

            if "Date" in df_combined.columns:
                df_combined["Date"] = pd.to_datetime(df_combined["Date"]).dt.strftime(
                    "%Y-%m-%d"
                )
                df_combined = df_combined.drop_duplicates(subset=["Date"], keep="last")
                df_combined = df_combined.sort_values("Date")

            # 新ファイル名
            latest_date_plain = latest_data_date_str.replace("-", "")
            new_filename = f"{code}_{current_file_fy}fy_{latest_date_plain}.csv"
            new_file_path = os.path.join(sector_dir, new_filename)

            logging.info(f"    Updated: {new_filename}")
            atomic_save(df_combined, new_file_path)

            # 旧ファイル削除
            if new_file_path != current_file_path:
                try:
                    os.remove(current_file_path)
                    file_repo.remove_file(code, current_file_path)
                except OSError as e:
                    logging.warning(f"    Warning: Failed to delete old file: {e}")

            file_repo.add_file(code, new_file_path)

        return "UPDATED"

    except Exception as e:
        logging.error(f"  Update failed for {code}: {e}")

        # tracebackはloggingには直接出せないので、文字列化して出すか、stderrに出す
        # ここではlogging.errorにexc_info=Trueをつける
        logging.error("Traceback:", exc_info=True)
        return "ERROR"


def update_topix_data(api_key, latest_biz_date, test_mode):
    """
    TOPIX指数データを更新する。
    """
    try:
        from src import config

        indices_dir = config.get_indices_dir()
        if not os.path.exists(indices_dir):
            os.makedirs(indices_dir, exist_ok=True)

        topix_file_path = config.get_topix_file_path()  # Path object
        topix_file_path_str = str(topix_file_path)

        last_date_str = None
        df_old = pd.DataFrame()

        if os.path.exists(topix_file_path_str):
            try:
                df_old = pd.read_csv(topix_file_path_str, dtype={"Date": str})
                if not df_old.empty and "Date" in df_old.columns:
                    last_date_str = df_old["Date"].max()  # YYYY-MM-DD
                    if "-" in last_date_str:
                        last_date_str = last_date_str.replace("-", "")
            except Exception as e:
                logging.warning(f"Failed to read existing TOPIX file: {e}")

        if last_date_str and last_date_str >= latest_biz_date:
            logging.info(f"TOPIX data is up to date ({last_date_str}).")
            return

        from_date_str = None
        if last_date_str:
            last_dt = datetime.datetime.strptime(last_date_str, "%Y%m%d")
            from_dt = last_dt + datetime.timedelta(days=1)
            from_date_str = from_dt.strftime("%Y%m%d")

        # If test mode, just log
        if test_mode:
            logging.info(
                f"[TEST] Would fetch TOPIX data from {from_date_str} to {latest_biz_date}"
            )
            return

        # Fetch data
        logging.info(f"Fetching TOPIX data: {from_date_str} - {latest_biz_date}")
        df_new = jq_api.get_topix(
            api_key, from_date=from_date_str, to_date=latest_biz_date
        )

        if df_new.empty:
            logging.info("No new TOPIX data found.")
            return

        # Merge and Save
        if not df_old.empty:
            if "Date" in df_new.columns:
                # Ensure date format consistency
                df_new["Date"] = pd.to_datetime(df_new["Date"]).dt.strftime("%Y-%m-%d")

            df_combined = pd.concat([df_old, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(
                subset=["Date"], keep="last"
            ).sort_values("Date")
        else:
            df_combined = df_new
            if "Date" in df_combined.columns:
                df_combined["Date"] = pd.to_datetime(df_combined["Date"]).dt.strftime(
                    "%Y-%m-%d"
                )
                df_combined = df_combined.sort_values("Date")

        atomic_save(df_combined, topix_file_path_str)
        logging.info(f"Updated TOPIX data. Total rows: {len(df_combined)}")

    except Exception as e:
        logging.error(f"Failed to update TOPIX data: {e}", exc_info=True)


def main():
    parser = argparse.ArgumentParser(description="J-Quants DB Updater")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in test mode (no actual API fetch/save, limited items)",
    )
    parser.add_argument(
        "--wait",
        type=float,
        default=1.2,
        help="Wait time (seconds) between API requests (min: 1.0, max: 2.0, default: 1.2)",
    )
    args = parser.parse_args()

    if not (1.0 <= args.wait <= 2.0):
        print(
            f"Error: wait_time must be between 1.0 and 2.0 seconds. Specified: {args.wait}"
        )
        sys.exit(1)

    print("=== J-Quants DB Updater Start ===")
    if args.test:
        print("!!! TEST MODE ENABLED !!!")

    # ロギング設定
    log_dir = os.path.join(ROOT_PATH, LOG_DIR_NAME)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filepath = os.path.join(log_dir, "update_jqdb.log")

    # logging設定: ファイル(上書き)とコンソール
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_filepath, mode="w", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logging.info("=== J-Quants DB Updater Start ===")
    if args.test:
        logging.info("!!! TEST MODE ENABLED !!!")

    logging.info("Authentication...")
    try:
        api_key = jq_api.get_api_key()
    except Exception as e:
        logging.error(f"Authentication Failed: {e}")
        return

    logging.info("Fetching Trading Calendar...")
    latest_biz_date = get_latest_business_day(api_key)
    logging.info(f"Latest Business Day: {latest_biz_date}")

    daily_quotes_dir, listed_info_dir = get_paths(ROOT_PATH)

    # Init File Repository
    try:
        file_repo = FileRepository(daily_quotes_dir)
    except Exception as e:
        logging.error(f"Failed to initialize FileRepository: {e}")
        return

    # 銘柄一覧準備
    listed_csv_path = os.path.join(listed_info_dir, "listed_info.csv")
    if os.path.exists(listed_csv_path):
        logging.info(f"Loading listed info from {listed_csv_path}")
        df_listed = pd.read_csv(listed_csv_path, dtype=str)
    else:
        logging.info("Listed info not found locally. Fetching from API...")
        try:
            df_listed = jq_api.get_listed_info(api_key)
            if not os.path.exists(listed_info_dir):
                os.makedirs(listed_info_dir)
            df_listed.to_csv(listed_csv_path, index=False, encoding="utf-8-sig")
        except Exception as e:
            logging.error(f"Failed to fetch listed info: {e}")
            return

    if "Code" not in df_listed.columns or "Sector33Code" not in df_listed.columns:
        logging.error("Error: Required columns (Code, Sector33Code) missing.")
        return

    total_brands = len(df_listed)
    logging.info(f"Total brands to process: {total_brands}")

    count_updated = 0
    count_skipped = 0
    count_error = 0

    target_markets = ["プライム", "スタンダード", "グロース"]

    for i, row in df_listed.iterrows():
        code = row["Code"]
        sector_code = row["Sector33Code"]
        market_name = row.get("MarketCodeName", "")

        if market_name not in target_markets:
            count_skipped += 1
            continue

        if args.test and i >= 10:
            logging.info("Test mode limit reached (10 items).")
            break

        if i % 50 == 0:
            logging.info(f"Processing... [{i}/{total_brands}]")

        result = update_file_for_code(
            api_key,
            code,
            sector_code,
            daily_quotes_dir,
            latest_biz_date,
            args.test,
            args.wait,
            file_repo,
        )

        if result == "UPDATED":
            count_updated += 1
        elif result == "ERROR":
            count_error += 1
        else:
            count_skipped += 1
            if args.test:
                # Reduce log noise
                pass

    # --- Update TOPIX Data ---
    logging.info("=== Updating TOPIX Data ===")
    update_topix_data(api_key, latest_biz_date, args.test)

    logging.info("=== J-Quants DB Updater Completed ===")
    logging.info(f"Updated: {count_updated}")
    logging.info(f"Skipped: {count_skipped}")
    logging.info(f"Errors: {count_error}")


if __name__ == "__main__":
    main()
