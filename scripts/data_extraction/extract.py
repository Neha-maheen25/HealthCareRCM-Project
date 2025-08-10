# scripts/data_extraction/extract.py
#this process extracts the raw files from hospital_a_db and hospital_b_db

import pandas as pd
import time
import sys
import os
import logging
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Add project root to sys.path
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(BASE_DIR)

from config.db_config import MYSQL_CONFIG


class DataExtractor:
    def __init__(self):
        self.engines = {}
        self.init_connections()

    def init_connections(self):
        for db_key in MYSQL_CONFIG:
            config = MYSQL_CONFIG[db_key]
            try:
                password = quote_plus(config["password"])
                conn_str = f"mysql+mysqlconnector://{config['user']}:{password}@{config['host']}:{config['port']}/{config['database']}"
                self.engines[db_key] = create_engine(conn_str)
                logging.info(f"✅ Connected to {db_key}")
            except Exception as e:
                logging.error(f"❌ Connection failed for {db_key}: {e}")

    def extract_patients(self):
        combined_df = pd.DataFrame()
        for db_key, engine in self.engines.items():
            try:
                start = time.time()
                df = pd.read_sql("SELECT * FROM patients", con=engine)
                # Normalize columns to lowercase and strip spaces
                df.columns = df.columns.str.strip().str.lower()

                # Rename columns to standard patient schema keys
                col_mapping = {
                    'id': 'patientid',
                    'patientid': 'patientid',
                    'f_name': 'firstname',
                    'firstname': 'firstname',
                    'l_name': 'lastname',
                    'lastname': 'lastname',
                    'm_name': 'middlename',
                    'middlename': 'middlename',
                    'ssn': 'ssn',
                    'phonenumber': 'phonenumber',
                    'phone_number': 'phonenumber',
                    'gender': 'gender',
                    'dob': 'dateofbirth',
                    'dateofbirth': 'dateofbirth',
                    'address': 'address',
                    'modifieddate': 'modifieddate'
                }
                df = df.rename(columns={col: col_mapping[col] for col in df.columns if col in col_mapping})

                # Add missing columns with empty strings
                required_cols = ['patientid', 'firstname', 'lastname', 'middlename', 'ssn',
                                 'phonenumber', 'gender', 'dateofbirth', 'address', 'modifieddate']
                for col in required_cols:
                    if col not in df.columns:
                        df[col] = ''

                df['source_hospital'] = db_key

                combined_df = pd.concat([combined_df, df], ignore_index=True)
                logging.info(f"✅ Extracted {len(df)} patients from {db_key} in {time.time() - start:.2f}s")
            except Exception as e:
                logging.error(f"❌ Failed to extract patients from {db_key}: {e}")

        # Create unified_patient_id with safe column checks
        if not combined_df.empty:
            # Ensure patientid column exists
            if 'patientid' not in combined_df.columns:
                logging.error("Missing 'patientid' column in combined patient data.")
                raise KeyError("Missing 'patientid' column in patients data after extraction.")

            combined_df['unified_patient_id'] = (
                combined_df['source_hospital'].str.replace('hospital_', '', regex=False).str.upper() + '-' +
                combined_df['patientid'].astype(str)
            )
            logging.info("✅ Created 'unified_patient_id' for patients across hospitals.")

        return combined_df

    def extract_transactions(self, start_date=None, end_date=None):
        combined_df = pd.DataFrame()
        for db_key, engine in self.engines.items():
            try:
                start = time.time()
                query = "SELECT * FROM transactions"
                if start_date and end_date:
                    query += f" WHERE transaction_date BETWEEN '{start_date}' AND '{end_date}'"
                df = pd.read_sql(query, con=engine)
                df['source_hospital'] = db_key
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                logging.info(f"✅ Extracted {len(df)} transactions from {db_key} in {time.time() - start:.2f}s")
            except Exception as e:
                logging.error(f"❌ Failed to extract transactions from {db_key}: {e}")
        return combined_df

    def extract_providers(self):
        combined_df = pd.DataFrame()
        for db_key, engine in self.engines.items():
            try:
                start = time.time()
                df = pd.read_sql("SELECT * FROM providers", con=engine)
                df['source_hospital'] = db_key
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                logging.info(f"✅ Extracted {len(df)} providers from {db_key} in {time.time() - start:.2f}s")
            except Exception as e:
                logging.error(f"❌ Failed to extract providers from {db_key}: {e}")
        return combined_df

    def extract_claims_data(self, folder_path, output_dir):
        for file in os.listdir(folder_path):
            if file.endswith(".csv"):
                try:
                    file_path = os.path.join(folder_path, file)
                    df = pd.read_csv(file_path)
                    df.columns = df.columns.str.strip().str.lower()

                    required_cols = {'claimid', 'patientid', 'claimamount', 'paidamount', 'claimstatus'}
                    if not required_cols.issubset(df.columns):
                        raise ValueError(f"Missing required columns in {file}")

                    # Save each hospital's cleaned claims CSV separately
                    output_file = os.path.join(output_dir, file.replace(".csv", "_extracted.csv"))
                    df.to_csv(output_file, index=False)

                    logging.info(f"✅ Extracted and saved {len(df)} claims to {output_file}")
                except Exception as e:
                    logging.error(f"❌ Error reading {file}: {e}")



if __name__ == "__main__":
    extractor = DataExtractor()

    patients_df = extractor.extract_patients()
    transactions_df = extractor.extract_transactions()
    providers_df = extractor.extract_providers()
    
    output_dir = os.path.join(BASE_DIR, "outputs", "extract_outputs")
    os.makedirs(output_dir, exist_ok=True)

    extractor.extract_claims_data(os.path.join(BASE_DIR, "claims"), output_dir)  # ✅ Fixed


    patients_df.to_csv(os.path.join(output_dir, "patients_extracted.csv"), index=False)
    transactions_df.to_csv(os.path.join(output_dir, "transactions_extracted.csv"), index=False)
    providers_df.to_csv(os.path.join(output_dir, "providers_extracted.csv"), index=False)
    
    logging.info(f"\n✅ All data extracted and saved to: {output_dir}")
