import argparse
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

import pandas as pd
import requests

from .categorization import categorize_state_processed_files
from .detail_extractor import download_and_process_district_html
from .settings import (
    BASE_URL,
    CREATION_ASSETS_DIR,
    DEFAULT_END_DATE,
    DEFAULT_FINANCIAL_YEAR,
    DEFAULT_START_DATE,
    HEADERS,
    NEW_BHUVAN_DIR,
    RAW_ASSETS_DIR,
)


LOGGER = logging.getLogger(__name__)
CSV_WRITE_LOCK = Lock()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("nrega_scraper.log"), logging.StreamHandler()],
    )


def fetch_data(url: str, data: dict, max_retries: int = 30):
    for attempt in range(max_retries):
        try:
            response = requests.post(url, data=data, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            if attempt == max_retries - 1:
                raise RuntimeError(f"Failed after {max_retries} attempts for {url}") from exc
            LOGGER.warning("Retry %s/%s for %s", attempt + 1, max_retries, url)
            time.sleep(2)


def get_districts(state_code: str):
    return fetch_data(
        BASE_URL + "location/getDistricts.php",
        {"username": "unauthourized", "state_code": state_code, "financial_year": DEFAULT_FINANCIAL_YEAR},
    )


def get_blocks(district_code: str):
    return fetch_data(
        BASE_URL + "location/getBlocks.php",
        {"username": "unauthourized", "district_code": district_code, "financial_year": DEFAULT_FINANCIAL_YEAR},
    )


def get_panchayats(block_code: str):
    return fetch_data(
        BASE_URL + "location/getPanchayats.php",
        {"username": "unauthourized", "block_code": block_code, "financial_year": DEFAULT_FINANCIAL_YEAR},
    )


def get_accepted_geotags(params: dict):
    return fetch_data(BASE_URL + "reports/accepted_geotags.php", params)


def get_start_date(state_name: str, district_name: str, block_name: str, panchayat_name: str) -> str:
    file_path = CREATION_ASSETS_DIR / state_name.upper() / f"{district_name.capitalize()}_latest_creation_times.xlsx"
    if not file_path.exists():
        return DEFAULT_START_DATE

    try:
        df = pd.read_excel(file_path, engine="openpyxl")
        row = df[(df["Panchayat"] == panchayat_name) & (df["Block"] == block_name)]
        if row.empty:
            return DEFAULT_START_DATE
        return str(row["creation_time"].iloc[0])
    except Exception:
        return DEFAULT_START_DATE


def save_district_data(state_name: str, district_name: str, data: pd.DataFrame) -> None:
    if data.empty:
        return

    output_dir = RAW_ASSETS_DIR / state_name.upper()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{district_name}_bhuvan_lat_lon.csv"

    with CSV_WRITE_LOCK:
        if output_file.exists():
            existing = pd.read_csv(output_file)
            merged = pd.concat([existing, data], ignore_index=True).drop_duplicates()
            merged.to_csv(output_file, index=False)
        else:
            data.to_csv(output_file, index=False)


def _process_panchayat(panchayat: dict, block_name: str, district_name: str, state_name: str, state_code: str, block_code: str):
    if panchayat.get("panchayat_code") == "All":
        return pd.DataFrame()

    params = {
        "username": "unauthourized",
        "stage": 0,
        "state_code": state_code,
        "district_code": block_code[:4],
        "block_code": block_code,
        "panchayat_code": panchayat["panchayat_code"],
        "financial_year": DEFAULT_FINANCIAL_YEAR,
        "accuracy": 0,
        "category_id": "All",
        "sub_category_id": "All",
        "start_date": get_start_date(state_name, district_name, block_name, panchayat["panchayat_name"]),
        "end_date": DEFAULT_END_DATE,
    }

    rows = get_accepted_geotags(params)
    if not rows or not isinstance(rows, list):
        return pd.DataFrame()

    for row in rows:
        row["State"] = state_name
        row["District"] = district_name
        row["Block"] = block_name
        row["Panchayat"] = panchayat["panchayat_name"]

    return pd.DataFrame(rows)


def process_state_raw(state_code: str, state_name: str, max_workers: int = 40) -> None:
    LOGGER.info("Starting raw scrape for %s", state_name)
    districts = get_districts(state_code)

    for district in districts:
        district_name = district.get("district_name")
        if district_name == "All":
            continue

        district_code = district["district_code"]
        blocks = get_blocks(district_code)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for block in blocks:
                if block.get("block_name") == "All":
                    continue

                block_name = block["block_name"]
                block_code = block["block_code"]
                panchayats = get_panchayats(block_code)

                for panchayat in panchayats:
                    futures.append(
                        executor.submit(
                            _process_panchayat,
                            panchayat,
                            block_name,
                            district_name,
                            state_name,
                            state_code,
                            block_code,
                        )
                    )

            for future in as_completed(futures):
                try:
                    df = future.result()
                    if not df.empty:
                        save_district_data(state_name, district_name, df)
                except Exception as exc:
                    LOGGER.warning("Panchayat worker failed: %s", exc)


def normalize_raw_columns(state_name: str) -> None:
    state_dir = RAW_ASSETS_DIR / state_name.upper()
    if not state_dir.exists():
        return

    mapping = {
        "assetid": "Asset ID",
        "workcode": "Work Code",
        "path1": "image_path1",
        "path2": "image_path2",
        "observername": "observer_name",
        "gpname": "Gram_panchayat",
        "creationtime": "creation_time",
    }

    desired_columns = [
        "State", "District", "Block", "collection_sno", "Asset ID", "Work Code", "serial_no",
        "image_path1", "image_path2", "accuracy", "observer_name", "Gram_panchayat", "creation_time",
        "lat", "lon", "Panchayat_ID", "Panchayat",
    ]

    for file in state_dir.glob("*_bhuvan_lat_lon.csv"):
        df = pd.read_csv(file)
        df.rename(columns=mapping, inplace=True)
        if "Panchayat_ID" not in df.columns:
            df["Panchayat_ID"] = None
        for col in desired_columns:
            if col not in df.columns:
                df[col] = None
        df = df[desired_columns]
        df.to_csv(file, index=False)


def merge_final_outputs(state_name: str, state_code: str) -> None:
    state_raw_dir = RAW_ASSETS_DIR / state_name.upper()
    NEW_BHUVAN_DIR.mkdir(parents=True, exist_ok=True)

    districts = [d["district_name"] for d in get_districts(state_code) if d.get("district_name") != "All"]
    columns_to_drop = {"collection_sno", "Category", "Sub-Category", "Work Type Cleaned", "serial_no", "accuracy"}

    for district in districts:
        work_file = state_raw_dir / f"{district}_work_data.csv"
        raw_file = state_raw_dir / f"{district}_bhuvan_lat_lon.csv"

        if not raw_file.exists():
            continue

        raw_df = pd.read_csv(raw_file)
        if work_file.exists():
            work_df = pd.read_csv(work_file)
            merged = pd.merge(raw_df, work_df, on="collection_sno", how="left")
        else:
            merged = raw_df

        merged = merged.drop(columns=[c for c in columns_to_drop if c in merged.columns], errors="ignore")
        if "Work Code" in merged.columns:
            merged = merged.drop_duplicates(subset="Work Code", keep="first")

        output_file = NEW_BHUVAN_DIR / f"{district.upper()}.csv"
        merged.to_csv(output_file, index=False)


def run_pipeline(state_dict: dict[str, str], max_workers: int = 40) -> None:
    configure_logging()
    for state_code, state_name in state_dict.items():
        process_state_raw(state_code, state_name, max_workers=max_workers)
        normalize_raw_columns(state_name)
        download_and_process_district_html(state_name, max_workers=max_workers)
        categorize_state_processed_files(state_name)
        merge_final_outputs(state_name, state_code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MGNREGA Bhuvan asset scraper")
    parser.add_argument("--state_dict", required=True, help="JSON dictionary of state_code to state_name")
    parser.add_argument("--max_workers", type=int, default=40)
    args = parser.parse_args()

    run_pipeline(json.loads(args.state_dict), max_workers=args.max_workers)
