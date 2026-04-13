# pyright: reportMissingImports=false

import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mgnrega_assets.categorization import categorize_state_processed_files
from mgnrega_assets.detail_extractor import download_and_process_district_html
from mgnrega_assets.pipeline import merge_final_outputs

LOG_FILE = Path(__file__).resolve().parents[1] / "nrega_scraper.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

if __name__ == "__main__":
    state_code = "05"
    state_name = "BIHAR"

    # Enrich existing raw CSVs with detail HTML extraction and merge outputs.
    download_and_process_district_html(state_name, max_workers=8)
    categorize_state_processed_files(state_name)
    merge_final_outputs(state_name, state_code)
