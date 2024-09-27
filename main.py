import os
import csv
from utils import fetch_metadata, download_and_process_csv_parallel, DOWNLOAD_DIR


def main():
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

    # Step 1: Download metadata
    metadata = fetch_metadata()

    # Step 2: Prepare tasks for parallel processing
    tasks = []
    for identifier, item in metadata.items():
        if 'distribution' in item and item['distribution']:
            download_url = item['distribution'][0]['downloadURL']
            last_modified = item['modified']
            # Extract the last part after the '/' in download_url
            file_name = os.path.basename(download_url)
            file_path = os.path.join(DOWNLOAD_DIR, file_name)
            tasks.append({
                'download_url': download_url,
                'file_path': file_path,
                'last_modified': last_modified
            })
        else:
            print(f"No distribution found for item with identifier {identifier}")

    # Step 3: Download and process CSV files in parallel
    download_and_process_csv_parallel(tasks)

    # Step 4: Write filtered items to a CSV file
    csv_file_path = os.path.join(DOWNLOAD_DIR, 'filtered_hospital_data.csv')
    with open(csv_file_path, mode='w', newline='') as csv_file:
        # Extract fieldnames from the first item in the metadata dictionary
        first_item = next(iter(metadata.values()), None)
        fieldnames = first_item.keys() if first_item else []
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for item in metadata.values():
            writer.writerow(item)


if __name__ == "__main__":
    main()
