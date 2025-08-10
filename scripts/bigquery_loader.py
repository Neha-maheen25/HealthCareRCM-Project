import os
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuration
PROJECT_ID = "healthcare-rcm-project-467704"
DATASET_ID = "healthcare_rcm"
KEY_PATH = "config/bigquery_key.json"

# Paths to outputs
DIM_PATH = "outputs/dim_outputs"
FACT_PATH = "outputs/fact_outputs"

# Authenticate
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

def load_csv_to_bigquery(file_path, table_name):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    print(f"🔄 Loading {file_path} → {table_id}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Replace if exists
    )

    with open(file_path, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    load_job.result()  # Wait for completion
    print(f"✅ Loaded {table_name} successfully.")

# Load all .csv files from dim_outputs
for filename in os.listdir(DIM_PATH):
    if filename.endswith(".csv"):
        table_name = filename.replace(".csv", "")
        file_path = os.path.join(DIM_PATH, filename)
        load_csv_to_bigquery(file_path, table_name)

# Load all .csv files from fact_outputs
for filename in os.listdir(FACT_PATH):
    if filename.endswith(".csv"):
        table_name = filename.replace(".csv", "")
        file_path = os.path.join(FACT_PATH, filename)
        load_csv_to_bigquery(file_path, table_name)
