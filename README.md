# MGNREGA Assets Scraper (Bihar)

Cleaned pipeline to scrape MGNREGA/Bhuvan asset data for Bihar and produce district-level merged outputs.

## Project Structure

- `src/mgnrega_assets/pipeline.py`: End-to-end pipeline runner
- `src/mgnrega_assets/detail_extractor.py`: HTML detail download and extraction per `collection_sno`
- `src/mgnrega_assets/categorization.py`: Work category tagging logic
- `scripts/run_bihar.py`: Quick runner for Bihar (`state_code=05`)
- `data/raw/assets/`: Raw district CSV outputs
- `data/interim/creation_assets/`: Optional latest creation-time input files
- `data/processed/new_bhuvan_files/`: Final merged district outputs

## Setup

```bash
pip install -r requirements.txt
pip install -e .
```

## Run Bihar Pipeline

```bash
python scripts/run_bihar.py
```

Or run with CLI:

```bash
python -m mgnrega_assets.pipeline --state_dict '{"05":"BIHAR"}' --max_workers 40
```

## Notes

- Output directories are intentionally gitignored; only placeholders are tracked.
- If creation-time Excel files are absent, the scraper falls back to a safe default start date.
