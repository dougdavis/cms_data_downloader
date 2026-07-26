# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

CMS Data Downloader fetches the CMS provider-data metastore, filters it down to hospital datasets, and downloads/normalizes the associated CSV files in parallel. It's a small, three-file script (no package structure, no CLI framework).

## Setup

```sh
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

`requirements.txt` only lists `pandas` and `requests`. The code also depends on `python-dateutil` (imported as `dateutil`) and `pytest` (for tests), neither of which is declared — install them manually if setup fails:

```sh
pip install python-dateutil pytest
```

## Commands

```sh
python main.py                                          # run the full download/process pipeline
pytest test_main.py                                      # run all tests
pytest test_main.py::test_download_and_process_csv       # run a single test
```

There is no lint/format tooling configured in this repo.

## Architecture

Two-module pipeline, orchestrated by `main.py` and implemented in `utils.py`:

1. **`fetch_metadata()`** (`utils.py`) — GETs the CMS metastore API (`METASTORE_URL`), keys the dataset list by `identifier`, and filters to items whose `theme` list contains `"Hospitals"`. This is the only filtering point; everything downstream operates on hospital datasets only.
2. **`main()`** (`main.py`) — for each filtered item, pulls `distribution[0]['downloadURL']` and `modified`, and builds a task dict (`download_url`, `file_path`, `last_modified`). Items without a `distribution` are skipped with a printed warning rather than failing the run.
3. **`download_and_process_csv_parallel()`** (`utils.py`) — runs one `download_and_process_csv()` call per task on a `ThreadPoolExecutor` (default worker count), and blocks on all futures.
4. **`download_and_process_csv()`** (`utils.py`) — per-file logic:
   - Skips the download if the file already exists **and** its local mtime is `>=` the source's `last_modified` (a filesystem-mtime cache check, not a content/hash check — deleting a file or touching its mtime backward forces a re-download).
   - Otherwise downloads the raw CSV, then rewrites it in place with `pandas`: column names are passed through `snake_case()` (strips non-alphanumeric chars, collapses whitespace to `_`, lowercases).
   - Certain columns (`column_12/14/17/19`) are forced to `dtype=str` on read to avoid pandas' mixed-type inference issues, with `low_memory=False`.
5. Back in `main()`, once all downloads finish, the full filtered metadata dict (not the CSVs) is written out to `downloads/filtered_hospital_data.csv` via `csv.DictWriter`, using the keys of the *first* metadata item as the fieldnames — so this assumes all items share the same key set.

All files land in `DOWNLOAD_DIR` ("downloads"), created if missing.

## Testing conventions

`test_main.py` mocks all network/IO boundaries — `main.fetch_metadata`, `main.download_and_process_csv_parallel`, and `utils.requests.get` — so the suite never makes real HTTP calls. Tests that write a real file to `downloads/` (e.g. `test_download_and_process_csv`) clean it up with `os.remove` at the end.
