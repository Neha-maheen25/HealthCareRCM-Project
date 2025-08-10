import os
from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()

# Define your dataset
dataset_id = "healthcare_rcm"

# Define the table paths and their corresponding local CSV file paths
tables_to_load = {
    "dim_date": "outputs/dim_outputs/dim_date.csv",
    "dim_patients": "outputs/dim_outputs/dim_patients.csv",
    "dim_procedures": "outputs/dim_outputs/dim_procedures.csv",
    "dim_providers": "outputs/dim_outputs/dim_providers.csv",
    "fact_claims": "outputs/fact_outputs/fact_claims.csv",
    "fact_transaction": "outputs/fact_outputs/fact_transactions.csv"
}

# Configure the job to skip the header row and autodetect schema
job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite existing tables
)

# Loop through each table and load its CSV
for table_name, csv_path in tables_to_load.items():
    table_id = f"{client.project}.{dataset_id}.{table_name}"
    print(f"🔄 Loading {csv_path} → {table_id}")

    try:
        with open(csv_path, "rb") as source_file:
            load_job = client.load_table_from_file(
                source_file, table_id, job_config=job_config
            )
        load_job.result()  # Wait for completion
        print(f"✅ Loaded {table_name} successfully.")
    except Exception as e:
        print(f"❌ Failed to load {table_name}: {e}")
