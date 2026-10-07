# Interactive MGNREGA / Bhuvan downloader

A desktop interface and command-line tool for downloading accepted asset geotags across the 34 states/UTs in the bundled Bhuvan catalog. Select any supported state and drill down to district, block, or panchayat. Queue several areas, including areas in different states, for one run.

There is no default state. Geographic coverage depends on the records available from Bhuvan; this is not a census of every MGNREGA work.

## Setup and launch

Requires Python 3.10+ with Tkinter. On Windows, include Tcl/Tk when installing Python. On Linux, install your distribution's Python Tk package if needed.

From the repository folder in PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe app.py
```

After setup, double-click **run_gui.cmd** on Windows. The launcher uses the project's virtual environment when available and reports missing dependencies rather than silently closing.

With the virtual environment activated, you can also use `mgnrega-assets`, `python -m mgnrega_assets`, or `python app.py`.

## Desktop workflow

1. Select a state/UT, then optionally a district, block, and panchayat. `All` includes every child within the chosen parent. Location lists load in the background.
2. Choose stage, financial year, category/subcategory, dates, and accuracy threshold.
3. Enable optional work details for names, categories, available costs/expenditures, and start dates. Missing fields remain blank. Detail requests make downloads slower.
4. Choose an output directory. Start the current selection or add multiple selections to the queue. A nonempty queue takes precedence over the form; each queued selection is a snapshot of its settings.
5. Cancel to stop; completed panchayats can be resumed. Closing during a download requests cancellation. Close again after it stops.

Save/Load settings stores one selection as JSON. Copy command saves the settings and copies a PowerShell command using the current Python executable. Refresh lists clears location choices below the state. Failed queued selections are logged, and remaining selections still run.

## CLI

```powershell
python app.py locations state
python app.py locations district --parent 32
python app.py scrape --state 32 --start-date 2024-01-01 --end-date 2024-12-31 --dry-run
python app.py scrape --config selection.json --details
python app.py scrape --help
```

Use the returned location codes with `--district`, `--block`, and `--panchayat`. Each narrower selection requires its parent. Supply `--state` or a state in a configuration file; the tool never silently chooses a state. JSON codes must be strings to preserve leading zeros.

Flags include `--financial-year`, `--stage`, `--category`, `--subcategory`, `--accuracy`, `--start-date`, `--end-date`, `--output`, `--details` / `--no-details`, and `--resume` / `--no-resume`. CLI arguments override JSON settings. `--dry-run` validates locally without making requests.

Stage codes: `0` Phase I assets; `1` Phase II before; `2` Phase II during; `3` Phase II after. Dates default to 2005-07-01 through today's local date. Accuracy follows the site's **greater than** filter; increasing it does not request better positional accuracy. Financial year and date range are separate server filters; geotag dates should not be interpreted as work start dates.

Exit codes: 0 complete, 2 partial, 1 failed or invalid configuration, 130 cancelled.

## Output and reproducibility

Each selection uses `data/downloads/<state>_<filter-hash>/` by default:

- `assets.csv`: all returned geotag records, location codes/names, stage, and optional details. Import codes as text in spreadsheets.
- `config.json`: exact settings.
- `panchayats/*.json`: atomic per-panchayat data/checkpoints, including valid empty responses.
- `status.json`: current completion status and failures.
- `run.log`: download progress and errors.

Repeated observations of a work are preserved; there is no work-code deduplication. CSV export is supported. Geographic summaries, XLSX/KML export, image downloads, and other dashboard reports are outside this tool's current scope.

Resume reuses the exact selection's snapshot; `--no-resume` refreshes it. Changing geography, filters, dates, stage, or detail mode creates a separate selection directory. Failed detail downloads retain their geotags with `detail_status=failed` and are retried on resume. Invalid server responses are failures rather than empty successes. Empty lists are valid zero-row results.

Requests are sequential, throttled, and retried with bounded timeouts. Cancellation can wait for an in-flight request. Memory use is proportional to a panchayat response rather than a whole state. Completed partition files survive failure/cancellation, but a previous `assets.csv` can remain until a new export finishes: check `status.json` before using it. Partial exports include successfully fetched partitions in that run.

The `.running` file prevents concurrent writers to the same selection. Following a crash, remove it only after verifying the previous process has stopped. Downloaded data is ignored by Git and never deleted by installation or repository maintenance.

## Development

```powershell
python -m unittest discover -s tests -v
python -m compileall -q src app.py
```

The tests use simulated responses, not bulk live downloads. GUI tests skip when Tk cannot open a display.

- `scraper.py`: configuration, API client, extraction, checkpoints, CSV export.
- `gui.py`: threaded desktop interface.
- `cli.py`: CLI shared with the GUI engine.
- `states.py`: supported state/UT catalog.

Runtime dependencies are declared once in `pyproject.toml`; `requirements.txt` installs the project. The source checkout and installed package use the same engine.
