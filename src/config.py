from pathlib import Path

# Project Root (calculated relative to this config file location)
# config.py is in <root>/src/config.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Base Configuration
BASE_OUTPUT_DIR = "market_analysis"
REPORTS_DIR_NAME = "reports"
JQDB_DIR_NAME = "jqdb"

# ...


def get_project_root():
    return PROJECT_ROOT


def get_base_dir():
    return PROJECT_ROOT / BASE_OUTPUT_DIR


def get_jqdb_dir():
    return PROJECT_ROOT / JQDB_DIR_NAME / "daily_quotes"


def get_indices_dir():
    return PROJECT_ROOT / JQDB_DIR_NAME / INDICES_DIR_NAME


def get_topix_file_path():
    return get_indices_dir() / TOPIX_FILE_NAME


# Sub-directories
SECTOR_DIR_NAME = "sector"
STOCKS_DIR_NAME = "stocks"
INDICES_DIR_NAME = "indices"

# Leaf directories
DATA_DIR_NAME = "data"
CHARTS_DIR_NAME = "charts"
RANK_DIR_NAME = "rank"
INDIV_DIR_NAME = "indiv"

# Files
TOPIX_FILE_NAME = "topix_daily.csv"


def get_reports_dir():
    return get_base_dir() / REPORTS_DIR_NAME


# --- Sector Analysis Paths ---
def get_sector_dir():
    return get_reports_dir() / SECTOR_DIR_NAME


def get_sector_data_dir():
    return get_sector_dir() / DATA_DIR_NAME


def get_sector_charts_dir():
    return get_sector_dir() / CHARTS_DIR_NAME


# --- Stock Analysis Paths ---
def get_stocks_dir():
    return get_reports_dir() / STOCKS_DIR_NAME


def get_stocks_data_dir():
    return get_stocks_dir() / DATA_DIR_NAME


def get_stocks_charts_dir():
    return get_stocks_dir() / CHARTS_DIR_NAME


def get_stocks_rank_dir():
    return get_stocks_dir() / RANK_DIR_NAME


def get_stocks_indiv_dir():
    return get_stocks_dir() / INDIV_DIR_NAME


# Ensure directories exist
def ensure_directories():
    for func in [
        get_sector_data_dir,
        get_sector_charts_dir,
        get_stocks_data_dir,
        get_stocks_charts_dir,
        get_stocks_rank_dir,
        get_stocks_indiv_dir,
        get_indices_dir,
    ]:
        path = func()
        path.mkdir(parents=True, exist_ok=True)
