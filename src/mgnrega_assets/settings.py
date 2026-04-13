from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_ASSETS_DIR = DATA_DIR / "raw" / "assets"
CREATION_ASSETS_DIR = DATA_DIR / "interim" / "creation_assets"
PROCESSED_DIR = DATA_DIR / "processed"
NEW_BHUVAN_DIR = PROCESSED_DIR / "new_bhuvan_files"

BASE_URL = "https://bhuvan-app2.nrsc.gov.in/mgnrega/nrega_dashboard_phase2/php/"
HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}

DEFAULT_START_DATE = "2005-07-01"
DEFAULT_END_DATE = "2025-01-01"
DEFAULT_FINANCIAL_YEAR = "All"
