# Interactive MGNREGA / Bhuvan downloader

Download accepted asset geotags through a desktop interface or the same underlying command-line engine. Location lists and asset filters come from the current Bhuvan Bhugram dashboard. The supplied `bhuvan_nrega_lat_lon.py` provides the 34 state/UT codes in `src/mgnrega_assets/states.py`.

## Start the desktop app (Windows PowerShell)

From this project folder:

```powershell
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements-desktop.txt
.venv/Scripts/python app.py
```

The local `.venv` has already been prepared on this machine. Tkinter is included in the standard Windows Python installer; enable its Tcl/Tk option if missing. Other systems may require their Python Tk package.

1. Select a state, then optionally a district, block, and panchayat. `All` downloads every child within the selected parent.
2. Choose stage, financial year, asset category/subcategory, dates, and accuracy threshold. Dropdowns load in the background. Changing a parent clears child selections.
3. Enable work details if needed. This makes one extra request per geotag and takes longer. Missing detail fields remain blank.
4. Choose an output folder. Start downloads for the current selection, or add several selections to the queue and start that queue. A nonempty queue takes precedence over the form. Queued configurations are snapshots; changing the form does not change them.
5. Use Cancel to stop. Resume reuses completed panchayats for exactly the same filters. Closing during a download requests cancellation; wait for it to stop, then close again.

Save/Load settings persists one selection as JSON. Copy command first saves that selection, then copies a PowerShell command; run it from the repository folder with the virtual environment activated (or replace `python` with `.venv/Scripts/python`). Loaded codes are validated against live parent lists when downloading. Refresh lists resets the geographic selection below state so you can choose a new area.

## CLI

```powershell
.venv/Scripts/python app.py --help
.venv/Scripts/python app.py locations state
.venv/Scripts/python app.py locations district --parent 05
.venv/Scripts/python app.py locations block --parent 0541
.venv/Scripts/python app.py locations panchayat --parent 0541006
.venv/Scripts/python app.py scrape --state 05 --district 0541 --block 0541006 --panchayat 0541006001 --start-date 2024-01-01 --end-date 2024-12-31
.venv/Scripts/python app.py scrape --config selection.json --details
.venv/Scripts/python app.py scrape --state 05 --stage 1 --dry-run
```

`--dry-run` validates configuration locally without requests. JSON settings use string codes (e.g. `"05"`); CLI arguments override supplied settings. Supported flags include `--financial-year`, `--category`, `--subcategory`, `--accuracy`, `--output`, `--details` / `--no-details`, and `--resume` / `--no-resume`.

Stage codes: `0` Phase I assets, `1` Phase II before, `2` Phase II during, `3` Phase II after. Dates default to 2005-07-01 through today's local date. The accuracy filter follows the website's **greater than** semantics; a larger threshold is not a request for better positional accuracy. Financial year and date range are separate server filters; the date is a geotag filter, not a guaranteed work-start-date filter.

Exit codes: 0 complete, 2 partial (some requests failed), 1 failed/invalid, 130 cancelled.

## Outputs and resume

Each selection has its own folder under `data/downloads/<state>_<filter-hash>/`:

- `assets.csv`: UTF-8 CSV with all returned geotag records, geography codes/names, stage, and optional work details. Codes may have leading zeros; import them as text in spreadsheet software.
- `config.json`: exact run settings.
- `panchayats/*.json`: atomic per-panchayat data/checkpoints, including successful empty responses.
- `status.json`: complete, partial, failed, cancelled, or running status, record counts when available, and failures.
- `run.log`: progress and request failure messages.

No work-code deduplication is performed. Multiple observations of the same work remain separate. These are accepted geotags, not a census of all MGNREGA works. Output is a combined CSV for the selected area; geographic summaries, XLSX, KML, image downloads, and other dashboard reports are not implemented.

Requests run sequentially with throttling, bounded retries, timeouts, and cancellation between requests. Cancellation may wait for the current HTTP request to finish. Memory use is bounded by a panchayat response rather than a whole state. Detail failures keep their geotags in the CSV with `detail_status=failed`; resume retries their panchayat. Unknown or invalid response shapes are failures, not empty successes. Actual empty lists are saved as zero-row successes.

The same exact selection resumes a snapshot; use `--no-resume` to refresh it. Different date ranges, stages, categories, detail modes, or locations have different directories. Failed/cancelled runs retain partition files. A previous `assets.csv` may remain until a new export finishes: always inspect `status.json`. A partial export contains only successfully fetched partitions from the current run. There is no automatic claim of complete coverage.

A `.running` file prevents simultaneous writers to one selection. After a process crash, remove it only after verifying the earlier process has stopped.

The new parser extracts fields it recognizes and leaves unavailable fields blank; it does not fabricate person-days, person counts, or expenditure totals. Raw API field names are preserved. The service can change availability or field definitions.

## Verification

```powershell
.venv/Scripts/python -m unittest discover -s tests -v
```

Tests cover configuration validation, filter-specific identity, repeated works, resume, retries, cancellation, parent membership, and missing detail fields. Live validation used a Bihar panchayat and the public category/year endpoints. No nationwide download is part of verification.

## Legacy pipeline

The older Bihar scripts and `mgnrega_assets.pipeline` remain available, with their original behavior. They use the separate dependencies in `requirements.txt` (including pandas 2.2.3; use a compatible Python such as 3.12). The new app does not use their fixed end date, district checkpoints, placeholder values, or final work-code deduplication. Existing legacy outputs are not automatically imported into the new checkpoint format.
