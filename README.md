# CMS Data Downloader

## Overview

The CMS Data Downloader is a Python project designed to download and process CSV files from specified URLs. The project includes functionality to download files in parallel and process them using `pandas`. It also includes unit tests to ensure the correctness of the code.

## Features

- Download CSV files from specified URLs
- Process CSV files using `pandas`
- Download and process files in parallel
- Unit tests using `pytest`

## Setup

### Prerequisites

- Python 3.6 or higher
- `pip` (Python package installer)

### Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/dougdavis/cms_data_downloader.git
    cd cms_data_downloader
    ```

2. Create a virtual environment:

    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3. Install the required packages:

    ```sh
    pip install -r requirements.txt
    ```

## Usage

1. Run the main script to download and process CSV files:

    ```sh
    python main.py
    ```

2. The downloaded and processed files will be saved in the specified download directory.

## Running Tests

To run the unit tests, use the following command:

```sh
pytest test_main.py
```

## Project Structure

cms_data_downloader/
│
├── main.py                 # Main script to download and process CSV files
├── utils.py                # Utility functions for downloading and processing CSV files
├── test_main.py            # Unit tests for the project
├── requirements.txt        # List of required packages
└── README.md               # Project documentation

## Example

Here is an example of how to use the download_and_process_csv function:

``sh
from utils import download_and_process_csv

download_url = '<http://example.com/file1.csv>'
file_path = 'path/to/download_dir/file1.csv'
last_modified = '2023-01-01T00:00:00Z'

