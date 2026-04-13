import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .settings import RAW_ASSETS_DIR


LOGGER = logging.getLogger(__name__)
DETAILS_URL = "https://bhuvan-app2.nrsc.gov.in/mgnrega/usrtasks/nrega_phase2/get/get_details.php"


def _download_html(collection_sno: str, html_path: Path, session: requests.Session) -> None:
    response = session.get(DETAILS_URL, params={"sno": collection_sno}, timeout=20)
    response.raise_for_status()
    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(response.text, encoding="utf-8")


def _extract_html_details(html_file: Path) -> dict:
    soup = BeautifulSoup(html_file.read_text(encoding="utf-8"), "html.parser")
    data = {
        "collection_sno": html_file.stem.split("_")[0],
        "Category": None,
        "Sub-Category": None,
        "Asset Name": None,
        "Work Name": None,
        "Work Type": None,
        "Estimated Cost": 0,
        "Start Location": -1,
        "End Location": -1,
        "Unskilled": 0,
        "Semi-Skilled": 0,
        "Skilled": 0,
        "Material": 0,
        "Total_Expenditure": 0,
        "Unskilled_Persondays": -1,
        "Semi-skilled_Persondays": -1,
        "Total_persondays": -1,
        "Unskilled_persons": -1,
        "Semi-skilled_persons": -1,
        "Total_persons": -1,
        "Work_start_date": -1,
        "HyperLink": -1,
    }

    cells = soup.find_all("td")
    for i in range(len(cells) - 1):
        key = cells[i].get_text(strip=True)
        val = cells[i + 1].get_text(strip=True)
        if key == "Category":
            data["Category"] = val
        elif key == "Sub-Category":
            data["Sub-Category"] = val
        elif key == "Asset Name":
            data["Asset Name"] = val
        elif key == "Work Name":
            data["Work Name"] = val
        elif key == "Work Type":
            data["Work Type"] = val
        elif key == "Cumulative Cost of Asset":
            data["Estimated Cost"] = val
        elif key == "Expenditure Unskilled":
            data["Unskilled"] = val
        elif key == "Expenditure Material/Skilled":
            data["Material"] = val
        elif key == "Work Start Date":
            data["Work_start_date"] = val

    if not data["Work Type"] and data["Sub-Category"]:
        data["Work Type"] = data["Sub-Category"]

    try:
        data["Total_Expenditure"] = float(data["Unskilled"] or 0) + float(data["Material"] or 0)
    except ValueError:
        data["Total_Expenditure"] = 0

    return data


def download_and_process_district_html(state_name: str, max_workers: int = 40) -> None:
    state_dir = RAW_ASSETS_DIR / state_name.upper()
    if not state_dir.exists():
        raise FileNotFoundError(f"State directory not found: {state_dir}")

    for district_csv in state_dir.glob("*_bhuvan_lat_lon.csv"):
        district = district_csv.name.replace("_bhuvan_lat_lon.csv", "")
        district_html_dir = state_dir / district / "html_files"

        df = pd.read_csv(district_csv)
        if "collection_sno" not in df.columns:
            LOGGER.warning("Skipping %s: collection_sno not found", district_csv.name)
            continue

        with requests.Session() as session:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for sno in df["collection_sno"].dropna().astype(str).unique():
                    html_file = district_html_dir / f"{sno}_work_data.html"
                    futures.append(executor.submit(_download_html, sno, html_file, session))

                for future in futures:
                    try:
                        future.result()
                    except Exception as exc:
                        LOGGER.warning("HTML download failed for %s: %s", district, exc)

        rows = []
        for html_file in district_html_dir.glob("*_work_data.html"):
            try:
                rows.append(_extract_html_details(html_file))
            except Exception as exc:
                LOGGER.warning("Parse failed for %s: %s", html_file.name, exc)

        if rows:
            pd.DataFrame(rows).to_csv(state_dir / f"{district}_processed.csv", index=False)
