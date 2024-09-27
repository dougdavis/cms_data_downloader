import os
import pytest
from unittest.mock import patch, MagicMock
from main import main
from utils import download_and_process_csv, download_and_process_csv_parallel, DOWNLOAD_DIR

@pytest.fixture
def mock_metadata():
    return {
        'item1': {
            'distribution': [{'downloadURL': 'http://example.com/file1.csv'}],
            'modified': '2023-01-01T00:00:00Z',
            'theme': ['Hospital']
        },
        'item2': {
            'distribution': [{'downloadURL': 'http://example.com/file2.csv'}],
            'modified': '2023-01-02T00:00:00Z',
            'theme': ['Clinic']
        }
    }

@patch('main.fetch_metadata')
@patch('main.download_and_process_csv_parallel')
def test_main(mock_download_and_process_csv_parallel, mock_fetch_metadata, mock_metadata):
    mock_fetch_metadata.return_value = mock_metadata

    # Run the main function
    main()

    # Check if the download_and_process_csv_parallel function was called with the correct arguments
    expected_tasks = [
        {
            'download_url': 'http://example.com/file1.csv',
            'file_path': os.path.join(DOWNLOAD_DIR, 'file1.csv'),
            'last_modified': '2023-01-01T00:00:00Z'
        },
        {
            'download_url': 'http://example.com/file2.csv',
            'file_path': os.path.join(DOWNLOAD_DIR, 'file2.csv'),
            'last_modified': '2023-01-02T00:00:00Z'
        }
    ]
    mock_download_and_process_csv_parallel.assert_called_once_with(expected_tasks)

@patch('utils.requests.get')
def test_download_and_process_csv(mock_get):
    from utils import download_and_process_csv

    # Mock the response from requests.get
    mock_response = MagicMock()
    mock_response.content = b'column12,column14,column17,column19\nvalue1,value2,value3,value4\n'
    mock_get.return_value = mock_response

    # Define the file path
    file_path = os.path.join(DOWNLOAD_DIR, 'file1.csv')

    # Call the function
    download_and_process_csv('http://example.com/file1.csv', file_path, '2023-01-01T00:00:00')

    # Check if the file was created and contains the expected content
    assert os.path.exists(file_path)
    with open(file_path, 'r') as file:
        content = file.read()
        assert content == 'column12,column14,column17,column19\nvalue1,value2,value3,value4\n'

    # Clean up the file
    os.remove(file_path)

