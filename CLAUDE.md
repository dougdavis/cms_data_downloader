# CLAUDE.md

This document provides guidance for AI assistants working with the CMS Data Downloader codebase.

## Project Overview

CMS Data Downloader is a Python application that downloads and processes CSV files from the Centers for Medicare & Medicaid Services (CMS) provider data API. It specifically filters for hospital-related datasets, processes them with pandas, and normalizes column names.

## Directory Structure

```
cms_data_downloader/
├── main.py              # Main entry point and orchestration
├── utils.py             # Core utility functions (API calls, downloads, processing)
├── test_main.py         # Unit tests using pytest
├── requirements.txt     # Python dependencies (pandas, requests, python-dateutil)
├── README.md            # User-facing documentation
├── .gitignore           # Git ignore patterns
└── downloads/           # Output directory (created at runtime)
```

## Core Files

### main.py
Entry point that orchestrates the workflow:
1. Creates download directory if needed
2. Fetches metadata from CMS API
3. Builds download tasks from metadata
4. Executes parallel downloads
5. Writes processed metadata to `filtered_hospital_data.csv`

### utils.py
Contains core functionality:
- `METASTORE_URL` - CMS API endpoint: `https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items`
- `DOWNLOAD_DIR` - Output directory: `downloads`
- `snake_case(s)` - Converts strings to snake_case for column normalization
- `fetch_metadata()` - Fetches and filters metadata for hospital datasets
- `download_and_process_csv()` - Downloads and processes individual CSV files
- `download_and_process_csv_parallel()` - Manages concurrent downloads using ThreadPoolExecutor

### test_main.py
Unit tests using pytest with unittest.mock for:
- `test_main()` - Tests main workflow with mocked dependencies
- `test_download_and_process_csv()` - Tests file download and processing

## Data Flow

```
CMS API → fetch_metadata() → Filter "Hospitals" theme → Build tasks
    ↓
download_and_process_csv_parallel() → ThreadPoolExecutor
    ↓
For each task:
    1. Check if file needs update (compare modified times)
    2. Download CSV from CMS
    3. Read with pandas (specific dtype handling for columns 12, 14, 17, 19)
    4. Normalize column names via snake_case()
    5. Write processed CSV
    ↓
Output: downloads/*.csv + filtered_hospital_data.csv
```

## Key Conventions

### Naming
- Functions and variables use `snake_case`
- Column names are normalized to lowercase snake_case
- Constants are `UPPER_SNAKE_CASE`

### Error Handling
- Uses `response.raise_for_status()` for HTTP error checking
- No try/except blocks; exceptions propagate to caller

### Code Style
- Imports grouped: standard library, then third-party packages
- Functions are small and focused on single responsibilities
- No type hints in current codebase

### CSV Processing
- Uses pandas for reading/writing CSV files
- Specific columns (12, 14, 17, 19) are forced to string dtype
- `low_memory=False` used to prevent dtype warnings

## Common Tasks

### Run the Application
```bash
python main.py
```

### Run Tests
```bash
pytest test_main.py
```

### Run Tests with Verbose Output
```bash
pytest test_main.py -v
```

### Setup Development Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
pip install pytest  # For testing (not in requirements.txt)
```

## Dependencies

| Package | Purpose |
|---------|---------|
| pandas | CSV reading, processing, and writing |
| requests | HTTP requests for API and file downloads |
| python-dateutil | ISO date parsing (imported in utils.py) |
| pytest | Testing framework (dev dependency, install separately) |

## API Integration

The application interacts with the CMS Provider Data API:
- **Endpoint**: `https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items`
- **Method**: GET
- **Response**: JSON array of dataset metadata objects
- **Filtering**: Items are filtered where `theme` array contains "Hospitals"
- **Download URLs**: Extracted from `distribution[0].downloadURL` field

### Metadata Structure
```python
{
    "identifier": "dataset-id",
    "theme": ["Hospitals", ...],
    "distribution": [{"downloadURL": "https://..."}],
    "modified": "2023-01-01T00:00:00Z",
    # ... other fields
}
```

## Output Files

- **downloads/*.csv** - Processed CSV files with normalized column names
- **downloads/filtered_hospital_data.csv** - Metadata for all downloaded hospital datasets

## Testing Guidelines

- Tests use `unittest.mock.patch` to mock external dependencies
- Mock `requests.get` for HTTP calls
- Mock `fetch_metadata` and `download_and_process_csv_parallel` in main tests
- Clean up created files after tests
- Use pytest fixtures for shared test data

## Important Notes for AI Assistants

1. **Incremental Downloads**: The application checks file modification times to avoid re-downloading unchanged files
2. **Parallel Processing**: Uses `ThreadPoolExecutor` for concurrent downloads - be mindful of thread safety
3. **Date Handling**: ISO dates with 'Z' suffix are converted to '+00:00' format for parsing
4. **Column dtypes**: Columns 12, 14, 17, 19 are explicitly set to string type to prevent type inference issues
5. **No Configuration File**: All configuration is via constants in `utils.py`
6. **External API Dependency**: Tests should mock the CMS API calls to avoid network dependencies
