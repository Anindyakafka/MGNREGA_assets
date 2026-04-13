import argparse
import json
import logging
import random
import re
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
CHECKPOINT_DIR = CREATION_ASSETS_DIR / "checkpoints"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("nrega_scraper.log"), logging.StreamHandler()],
    )


def fetch_data(url: str, data: dict, max_retries: int = 30, base_delay: float = 1.0, max_delay: float = 20.0):
    for attempt in range(max_retries):
        try:
            response = requests.post(url, data=data, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            if attempt == max_retries - 1:
                raise RuntimeError(f"Failed after {max_retries} attempts for {url}") from exc
            backoff = min(max_delay, base_delay * (2 ** attempt))
            jitter = random.uniform(0.0, 0.3 * backoff)
            sleep_time = backoff + jitter
            LOGGER.warning(
                "Retry %s/%s for %s in %.2fs",
                attempt + 1,
                max_retries,
                url,
                sleep_time,
            )
            time.sleep(sleep_time)


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


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._ -]", "_", str(value)).strip()


def _checkpoint_path(state_name: str) -> Path:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    return CHECKPOINT_DIR / f"{state_name.upper()}_raw_scrape_checkpoint.json"


def _load_checkpoint(state_name: str) -> dict:
    cp_path = _checkpoint_path(state_name)
    if not cp_path.exists():
        return {"completed_districts": []}
    try:
        return json.loads(cp_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"completed_districts": []}


def _save_checkpoint(state_name: str, checkpoint_data: dict) -> None:
    cp_path = _checkpoint_path(state_name)
    cp_path.write_text(json.dumps(checkpoint_data, indent=2), encoding="utf-8")


def _mark_district_complete(state_name: str, district_name: str) -> None:
    checkpoint = _load_checkpoint(state_name)
    completed = set(checkpoint.get("completed_districts", []))
    completed.add(district_name)
    checkpoint["completed_districts"] = sorted(completed)
    _save_checkpoint(state_name, checkpoint)


def reset_checkpoint(state_name: str) -> None:
    cp_path = _checkpoint_path(state_name)
    if cp_path.exists():
        cp_path.unlink()


def get_start_date(state_name: str, district_name: str, block_name: str, panchayat_name: str) -> str:
    district_stem = safe_name(district_name)
    file_path = CREATION_ASSETS_DIR / state_name.upper() / f"{district_stem.capitalize()}_latest_creation_times.xlsx"
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
    district_stem = safe_name(district_name)
    output_file = output_dir / f"{district_stem}_bhuvan_lat_lon.csv"

    with CSV_WRITE_LOCK:
        io_retries = 8
        for attempt in range(io_retries):
            try:
                if output_file.exists():
                    existing = pd.read_csv(output_file)
                    merged = pd.concat([existing, data], ignore_index=True).drop_duplicates()
                    merged.to_csv(output_file, index=False)
                else:
                    data.to_csv(output_file, index=False)
                return
            except PermissionError:
                if attempt == io_retries - 1:
                    raise
                time.sleep(1.0 + attempt * 0.5)


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


def process_state_raw(state_code: str, state_name: str, max_workers: int = 40, resume: bool = True) -> None:
    LOGGER.info("Starting raw scrape for %s", state_name)
    districts = get_districts(state_code)
    completed = set()
    if resume:
        completed = set(_load_checkpoint(state_name).get("completed_districts", []))
        LOGGER.info("Loaded checkpoint for %s with %s completed districts", state_name, len(completed))

    for district in districts:
        district_name = district.get("district_name")
        if district_name == "All":
            continue
        if resume and district_name in completed:
            LOGGER.info("Skipping %s due to checkpoint", district_name)
            continue

        district_code = district["district_code"]
        blocks = get_blocks(district_code)
        district_chunks = []
        district_failures = 0

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
                        district_chunks.append(df)
                except Exception as exc:
                    district_failures += 1
                    LOGGER.warning("Panchayat worker failed: %s", exc)

        if district_chunks:
            try:
                district_df = pd.concat(district_chunks, ignore_index=True).drop_duplicates()
                save_district_data(state_name, district_name, district_df)
            except Exception as exc:
                district_failures += 1
                LOGGER.warning("Failed to save district %s: %s", district_name, exc)

        if district_failures == 0:
            _mark_district_complete(state_name, district_name)
            LOGGER.info("Completed district %s", district_name)
        else:
            LOGGER.warning("District %s had %s failures and was not checkpointed", district_name, district_failures)


def generate_latest_creation_time_workbooks(state_name: str) -> None:
    state_dir = RAW_ASSETS_DIR / state_name.upper()
    if not state_dir.exists():
        return

    out_dir = CREATION_ASSETS_DIR / state_name.upper()
    out_dir.mkdir(parents=True, exist_ok=True)

    for raw_file in state_dir.glob("*_bhuvan_lat_lon.csv"):
        df = pd.read_csv(raw_file)
        required_cols = {"creation_time", "Panchayat", "Block"}
        if not required_cols.issubset(df.columns):
            continue

        if "Panchayat_ID" not in df.columns:
            df["Panchayat_ID"] = None

        df["creation_time"] = pd.to_datetime(df["creation_time"], errors="coerce", dayfirst=True)
        agg = df.dropna(subset=["creation_time"]).groupby(["Panchayat_ID", "Panchayat", "Block"], as_index=False)["creation_time"].max()
        agg["creation_time"] = agg["creation_time"].dt.date

        district = raw_file.name.replace("_bhuvan_lat_lon.csv", "")
        output_file = out_dir / f"{district.capitalize()}_latest_creation_times.xlsx"
        agg.to_excel(output_file, index=False)


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
    columns_to_drop = {"collection_sno", "Work Type Cleaned", "serial_no", "accuracy"}

    for district in districts:
        district_stem = safe_name(district)
        work_file = state_raw_dir / f"{district_stem}_work_data.csv"
        raw_file = state_raw_dir / f"{district_stem}_bhuvan_lat_lon.csv"

        if not raw_file.exists():
            continue

        raw_df = pd.read_csv(raw_file)
        if work_file.exists():
            work_df = pd.read_csv(work_file)
            merged = pd.merge(raw_df, work_df, on="collection_sno", how="left")
        else:
            merged = raw_df

        # Keep original labels and expose clearer aliases for downstream users.
        if "Category" in merged.columns and "Asset Type" not in merged.columns:
            merged["Asset Type"] = merged["Category"]
        if "Sub-Category" in merged.columns and "Asset Sub-Type" not in merged.columns:
            merged["Asset Sub-Type"] = merged["Sub-Category"]

        merged = merged.drop(columns=[c for c in columns_to_drop if c in merged.columns], errors="ignore")
        if "Work Code" in merged.columns:
            merged = merged.drop_duplicates(subset="Work Code", keep="first")

        output_file = NEW_BHUVAN_DIR / f"{district_stem.upper()}.csv"
        merged.to_csv(output_file, index=False)


def run_smoke_test(state_code: str = "05", state_name: str = "BIHAR") -> None:
    RAW_ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    CREATION_ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    NEW_BHUVAN_DIR.mkdir(parents=True, exist_ok=True)

    districts = get_districts(state_code)
    if not districts or not isinstance(districts, list):
        raise RuntimeError(f"Smoke test failed: no districts returned for {state_name}")

    first_district = next((d for d in districts if d.get("district_name") != "All"), None)
    if not first_district:
        raise RuntimeError("Smoke test failed: no usable district found")

    blocks = get_blocks(first_district["district_code"])
    first_block = next((b for b in blocks if b.get("block_name") != "All"), None)
    if not first_block:
        raise RuntimeError("Smoke test failed: no usable block found")

    panchayats = get_panchayats(first_block["block_code"])
    first_panchayat = next((p for p in panchayats if p.get("panchayat_code") != "All"), None)
    if not first_panchayat:
        raise RuntimeError("Smoke test failed: no usable panchayat found")

    payload = {
        "username": "unauthourized",
        "stage": 0,
        "state_code": state_code,
        "district_code": first_block["block_code"][:4],
        "block_code": first_block["block_code"],
        "panchayat_code": first_panchayat["panchayat_code"],
        "financial_year": DEFAULT_FINANCIAL_YEAR,
        "accuracy": 0,
        "category_id": "All",
        "sub_category_id": "All",
        "start_date": DEFAULT_START_DATE,
        "end_date": DEFAULT_END_DATE,
    }
    _ = get_accepted_geotags(payload)

    LOGGER.info(
        "Smoke test passed for %s (%s): district=%s block=%s panchayat=%s",
        state_name,
        state_code,
        first_district["district_name"],
        first_block["block_name"],
        first_panchayat["panchayat_name"],
    )


def run_pipeline(state_dict: dict[str, str], max_workers: int = 40, resume: bool = True, reset_cp: bool = False) -> None:
    configure_logging()
    for state_code, state_name in state_dict.items():
        if reset_cp:
            reset_checkpoint(state_name)
        process_state_raw(state_code, state_name, max_workers=max_workers, resume=resume)
        normalize_raw_columns(state_name)
        generate_latest_creation_time_workbooks(state_name)
        download_and_process_district_html(state_name, max_workers=max_workers)
        categorize_state_processed_files(state_name)
        merge_final_outputs(state_name, state_code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MGNREGA Bhuvan asset scraper")
    parser.add_argument("--state_dict", required=True, help="JSON dictionary of state_code to state_name")
    parser.add_argument("--max_workers", type=int, default=40)
    parser.add_argument("--no_resume", action="store_true", help="Disable resume from district checkpoint")
    parser.add_argument("--reset_checkpoint", action="store_true", help="Delete existing state checkpoint before run")
    parser.add_argument("--smoke_test", action="store_true", help="Run smoke test only")
    args = parser.parse_args()

    parsed_state_dict = json.loads(args.state_dict)
    if args.smoke_test:
        first_state_code = next(iter(parsed_state_dict))
        run_smoke_test(first_state_code, parsed_state_dict[first_state_code])
    else:
        run_pipeline(
            parsed_state_dict,
            max_workers=args.max_workers,
            resume=not args.no_resume,
            reset_cp=args.reset_checkpoint,
        )
