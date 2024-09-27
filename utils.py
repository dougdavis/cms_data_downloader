import os
import re
import requests
import pandas as pd
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor
from dateutil import parser

# Constants
METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
DOWNLOAD_DIR = "downloads"
METADATA_FILE = "metadata.json"


def snake_case(s):
    s = re.sub(r'[^a-zA-Z0-9\s]', '', s)
    s = re.sub(r'\s+', '_', s)
    return s.lower()


def fetch_metadata():
    response = requests.get(METASTORE_URL)
    response.raise_for_status()
    metadata = json.loads(response.text)
    # Convert the list to a dictionary using 'identifier' as the key
    metadata_dict = {item['identifier']: item for item in metadata}
    # Filter the dictionary to only include items where theme contains "Hospitals"
    hospital_items = {identifier: item for identifier, item in metadata_dict.items() if "Hospitals" in item.get('theme', [])}
    return hospital_items


def download_and_process_csv(download_url, file_name, last_modified):
    # Check if file needs to be downloaded
    if os.path.exists(file_name):
        file_modified_time = datetime.fromtimestamp(os.path.getmtime(file_name))
        if file_modified_time >= datetime.fromisoformat(last_modified):
            return

    # Download the file
    response = requests.get(download_url)
    print(f"Downloading {download_url} to {file_name}")
    response.raise_for_status()
    with open(file_name, 'wb') as f:
        f.write(response.content)
    # Read the CSV file with specified dtypes and low_memory=False
    dtype = {
        'column_12': str,
        'column_14': str,
        'column_17': str,
        'column_19': str
    }

    # Process the file using pandas
    df = pd.read_csv(file_name, dtype=dtype, low_memory=False)
    df.columns = [snake_case(col) for col in df.columns]
    df.to_csv(file_name, index=False)

    # Convert last_modified to a valid ISO format string
    if last_modified.endswith('Z'):
        last_modified = last_modified[:-1] + '+00:00'

    # Parse the last_modified date
    last_modified_date = parser.isoparse(last_modified)
    print(f"File {file_name} was last modified on {last_modified_date}")


def download_and_process_csv_parallel(tasks):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(download_and_process_csv, task['download_url'], task['file_path'], task['last_modified']) for task in tasks]
        for future in futures:
            future.result()  # Wait for all futures to complete
