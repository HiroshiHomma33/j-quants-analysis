import os

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.jquants.com/v2"


def _get_session() -> requests.Session:
    """
    リトライ設定を行ったrequests.Sessionオブジェクトを作成して返す。
    """
    session = requests.Session()
    retries = Retry(
        total=3,  # 最大リトライ回数
        backoff_factor=1,  # リトライ間隔の係数 (1s, 2s, 4s...)
        status_forcelist=[500, 502, 503, 504],  # リトライ対象のステータスコード
        allowed_methods=["GET", "POST"],  # リトライ対象のメソッド
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_api_key() -> str:
    """
    環境変数からAPIキーを取得して返す。
    """
    api_key = os.environ.get("JQ_API_KEY")

    if not api_key:
        raise ValueError("環境変数 JQ_API_KEY が設定されていません。")

    return api_key


def _get_headers(api_key: str) -> dict:
    return {"x-api-key": api_key}


def get_listed_info(
    api_key: str, code: str | None = None, date: str | None = None
) -> pd.DataFrame:
    """
    上場銘柄一覧を取得し、DataFrameとして返す。
    """
    url = f"{BASE_URL}/equities/master"
    headers = _get_headers(api_key)
    session = _get_session()

    # パラメータ対応（code, date）
    params = {}
    if code:
        params["code"] = code
    if date:
        params["date"] = date

    response = session.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()

    resp_json = response.json()
    # V2仕様: {"data": [...]}
    data = resp_json.get("data", [])

    df = pd.DataFrame(data)

    # カラム名マッピング (V2 -> 既存CSVヘッダー互換)
    rename_map = {
        "Date": "Date",
        "Code": "Code",
        "CoName": "CompanyName",
        "CoNameEn": "CompanyNameEnglish",
        "S17": "Sector17Code",
        "S17Nm": "Sector17CodeName",
        "S33": "Sector33Code",
        "S33Nm": "Sector33CodeName",
        "ScaleCat": "ScaleCategory",
        "Mkt": "MarketCode",
        "MktNm": "MarketCodeName",
    }
    # 存在しないカラムがあってもエラーにならないように rename する
    df = df.rename(columns=rename_map)

    # 必要なカラム順序に並べ替える（既存CSVとの互換性のため）
    expected_columns = [
        "Date",
        "Code",
        "CompanyName",
        "CompanyNameEnglish",
        "Sector17Code",
        "Sector17CodeName",
        "Sector33Code",
        "Sector33CodeName",
        "ScaleCategory",
        "MarketCode",
        "MarketCodeName",
    ]

    # マッピング後のDataFrameに含まれるカラムのみ抽出して並べ替え（不足カラムは無視、またはNaNで追加も検討だが、基本はAPI戻り値依存）
    # APIのレスポンスが完全であればすべて揃うはず
    available_cols = [c for c in expected_columns if c in df.columns]
    df = df[available_cols]

    return df


def get_daily_quotes(
    api_key: str,
    code: str | None = None,
    date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> pd.DataFrame:
    """
    株価四本値を取得し、DataFrameとして返す。
    ページネーションに対応し、全データを取得する。
    V2 APIを使用し、カラム名をV1互換に戻す。
    """
    url = f"{BASE_URL}/equities/bars/daily"
    headers = _get_headers(api_key)
    params = {}
    if code:
        params["code"] = code
    if date:
        params["date"] = date
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date

    session = _get_session()
    all_data = []

    while True:
        response = session.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        resp_json = response.json()

        # 仕様準拠: data キーを優先
        daily_quotes = resp_json.get("data", [])
        if not daily_quotes:
            # Fallback for older spec or mixed behavior
            daily_quotes = resp_json.get("daily_quotes", [])

        if not daily_quotes:
            print(f"DEBUG: Empty daily_quotes. JSON: {resp_json}")

        all_data.extend(daily_quotes)

        pagination_key = resp_json.get("pagination_key")
        if pagination_key:
            params["pagination_key"] = pagination_key
        else:
            break

    df = pd.DataFrame(all_data)

    # カラム名マッピング (V2 -> V1)
    # V2のキー: Date, Code, O, H, L, C, UL, LL, Vo, Va, AdjFactor, AdjO, AdjH, AdjL, AdjC, AdjVo
    rename_map = {
        "O": "Open",
        "H": "High",
        "L": "Low",
        "C": "Close",
        "UL": "UpperLimit",
        "LL": "LowerLimit",
        "Vo": "Volume",
        "Va": "TurnoverValue",
        "AdjFactor": "AdjustmentFactor",
        "AdjO": "AdjustmentOpen",
        "AdjH": "AdjustmentHigh",
        "AdjL": "AdjustmentLow",
        "AdjC": "AdjustmentClose",
        "AdjVo": "AdjustmentVolume",
    }
    df = df.rename(columns=rename_map)

    return df


def get_topix(
    api_key: str, from_date: str | None = None, to_date: str | None = None
) -> pd.DataFrame:
    """
    TOPIX指数四本値を取得し、DataFrameとして返す。
    ページネーション対応。
    """
    url = f"{BASE_URL}/indices/bars/daily/topix"  # V2 endpoint
    headers = _get_headers(api_key)
    params = {}
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date

    session = _get_session()
    all_data = []

    while True:
        response = session.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        resp_json = response.json()

        topix_data = resp_json.get("data", [])
        if not topix_data:
            topix_data = resp_json.get("topix", [])
        all_data.extend(topix_data)

        pagination_key = resp_json.get("pagination_key")
        if pagination_key:
            params["pagination_key"] = pagination_key
        else:
            break

    df = pd.DataFrame(all_data)

    # TOPIXも同様にカラム短縮されている可能性があるためマッピング
    rename_map = {
        "O": "Open",
        "H": "High",
        "L": "Low",
        "C": "Close",
    }
    df = df.rename(columns=rename_map)

    return df


def get_fins_statements(
    api_key: str, code: str | None = None, date: str | None = None
) -> pd.DataFrame:
    """
    財務情報を取得し、DataFrameとして返す。
    ページネーション対応。
    """
    url = f"{BASE_URL}/fins/summary"  # changed from statements to summary? Check spec carefully or use 'details'
    # The migration doc said /v1/fins/statements -> /v2/fins/summary

    headers = _get_headers(api_key)
    params = {}
    if code:
        params["code"] = code
    if date:
        params["date"] = date

    session = _get_session()
    all_data = []

    while True:
        response = session.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        resp_json = response.json()

        # 仕様準拠: data キーを優先
        statements = resp_json.get("data", [])
        if not statements:
            # Fallback
            statements = resp_json.get("fins_summary", [])
        if not statements:
            statements = resp_json.get("statements", [])

        all_data.extend(statements)

        pagination_key = resp_json.get("pagination_key")
        if pagination_key:
            params["pagination_key"] = pagination_key
        else:
            break

    df = pd.DataFrame(all_data)
    return df


def get_market_code(code_name: str) -> str:
    code_dict = {
        "東証一部": "0101",
        "東証二部": "0102",
        "マザーズ": "0104",
        "その他": "0109",
        "プライム": "0111",
        "スタンダード": "0112",
        "グロース": "0113",
    }
    return_code = code_dict.get(code_name, None)
    if return_code:
        return return_code
    else:
        raise ValueError(f"指定されたマーケットコード({code_name})は存在しません。")


def get_trading_calendar(
    api_key: str,
    hol_div: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> pd.DataFrame:
    """
    取引カレンダーを取得し、DataFrameとして返す。
    """
    url = f"{BASE_URL}/markets/calendar"
    headers = _get_headers(api_key)
    params = {}
    if hol_div:
        params["hol_div"] = hol_div
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date

    session = _get_session()

    response = session.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    resp_json = response.json()

    # 仕様準拠: data キーを優先
    data = resp_json.get("data", [])
    if not data:
        data = resp_json.get("trading_calendar", [])

    df = pd.DataFrame(data)

    # 仕様準拠: HolDiv -> HolidayDivision (既存コードとの互換性維持)
    if "HolDiv" in df.columns:
        df = df.rename(columns={"HolDiv": "HolidayDivision"})

    return df
