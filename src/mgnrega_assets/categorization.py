import os
import re
from pathlib import Path

import pandas as pd

from .settings import RAW_ASSETS_DIR


CATEGORY_KEYWORDS = {
    "Irrigation - Site level impact": [
        "bund", "bandh", "tank", "irrigation", "well-filter", "talab-fish", "pond-fish", "percolation",
        "desilting", "sichai kup", "sinchai kup", "nali nirman"
    ],
    "SWC - Landscape level impact": [
        "aahar", "ahar", "dam", "terrace", "trench", "diversion", "gabion", "canal-plantation", "nali",
        "channel", "embank", "dyke", "watercourse", "soak", "spur", "silviculture", "reclamation land"
    ],
    "Plantation": [
        "plantation", "tree", "forestry", "nursery", "forest", "grass", "afforestation", "horticulture"
    ],
    "Household Livelihood": [
        "shelter", "fishery pond", "cattle", "goat", "poultry", "piggery", "livestock", "fish"
    ],
    "Agri Impact - HH, Community": [
        "land levelling", "land leveling", "land development", "compost pit", "fallow land", "storage", "vermi",
        "nallah", "pmayg", "miti bharai", "samtali karan"
    ],
    "Others - HH, Community": [
        "cement concrete", "kharanja", "haat", "anganwadi", "toilet", "shed", "wall", "kitchen", "bhavan",
        "road", "school", "awaas", "seva kendra", "fencing", "play ground", "ihhl", "public assets", "pcc", "rcc"
    ],
    "Irrigation Site level - Non RWH": ["filter", "boring"],
}

STOP_WORDS = {
    "i", "me", "my", "we", "our", "you", "your", "he", "him", "his", "she", "her", "it", "its", "they",
    "them", "their", "what", "which", "who", "this", "that", "these", "those", "am", "is", "are", "was", "were",
    "be", "been", "being", "have", "has", "had", "do", "does", "did", "a", "an", "the", "and", "but", "if", "or",
    "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through",
    "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under",
    "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few",
    "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very"
}


def _remove_special_chars(value: str) -> str:
    line = value
    for ch in "?,&%@()/_[]{}$#!^*+=|;<>:":
        line = line.replace(ch, " ")
    line = re.sub(r"\d", " ", line)
    return line.strip()


def _clean(value: str) -> str:
    text = _remove_special_chars(str(value).lower())
    words = [w for w in text.split() if len(w) > 2 and w not in STOP_WORDS]
    return " ".join(words)


def categorize_dataframe(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()
    for col in ["Work Name", "Asset Name", "Work Type"]:
        cleaned_col = f"{col} Cleaned"
        data[cleaned_col] = data[col].fillna("").apply(_clean)

    data["WorkCategory"] = ""

    for idx, row in data.iterrows():
        haystack = " ".join([
            row.get("Work Name Cleaned", ""),
            row.get("Asset Name Cleaned", ""),
            row.get("Work Type Cleaned", ""),
        ])
        matched_category = ""
        for category, keywords in CATEGORY_KEYWORDS.items():
            if any(keyword in haystack for keyword in keywords):
                matched_category = category
                break
        data.at[idx, "WorkCategory"] = matched_category

    return data.drop(columns=["Work Name Cleaned", "Asset Name Cleaned", "Work Type Cleaned"], errors="ignore")


def categorize_state_processed_files(state_name: str) -> None:
    state_dir = RAW_ASSETS_DIR / state_name.upper()
    if not state_dir.exists():
        raise FileNotFoundError(f"State directory not found: {state_dir}")

    for filename in os.listdir(state_dir):
        if not filename.endswith("_processed.csv"):
            continue

        input_path = state_dir / filename
        district_name = filename.replace("_processed.csv", "")
        output_path = state_dir / f"{district_name}_work_data.csv"
        blank_output_path = state_dir / f"{district_name}_blank_data.csv"

        df = pd.read_csv(input_path)
        result = categorize_dataframe(df)
        result.to_csv(output_path, index=False)

        blank = result[result["WorkCategory"].isna() | (result["WorkCategory"] == "")]
        blank[["Asset Name", "Work Name", "Work Type"]].to_csv(blank_output_path, index=False)
