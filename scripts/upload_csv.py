"""
AIAP CLI — Data Upload Utility
Quickly upload CSV metrics to the intelligent ingestion API.
"""
import requests
import os
import sys

# Add project root to path for portable config import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import Config

def upload_csv(file_path, url="http://localhost:5001/api/v1/data/upload/metrics"):
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        return

    print(f"--- UPLOADING {os.path.basename(file_path)} ---")
    try:
        with open(file_path, 'rb') as f:
            files = {'file': f}
            response = requests.post(url, files=files)
            
            if response.status_code == 201:
                data = response.json()
                print(f"SUCCESS: {data['message']}")
                print(f"Rows Processed: {data['rows_processed']}")
                print(f"API Status: {data['status']}")
            else:
                print(f"FAILURE ({response.status_code}): {response.text}")
    except Exception as e:
        print(f"Error connecting to AIAP server: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        # Default to the fixture file if no path provided
        default_path = os.path.join(Config.RAW_DATA_DIR, "atm_fixture_daily_metrics.csv")
        if os.path.exists(default_path):
            upload_csv(default_path)
        else:
            print("Usage: python scripts/upload_csv.py <path_to_csv>")
    else:
        upload_csv(sys.argv[1])
