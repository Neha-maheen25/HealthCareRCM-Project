# scripts/test_bigquery.py
#this script tells ous if we are connected to bigquery successfully

from google.cloud import bigquery
import os

#  Update the path to your actual key file
key_path = "config/bigquery_key.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

def test_bigquery():
    try:
        client = bigquery.Client()
        datasets = list(client.list_datasets())
        if datasets:
            print("Successfully connected to BigQuery. Datasets:")
            for ds in datasets:
                print(f" - {ds.dataset_id}")
        else:
            print("Connected to BigQuery, but no datasets found.")
    except Exception as e:
        print(f"BigQuery connection failed: {e}")

if __name__ == "__main__":
    test_bigquery()
