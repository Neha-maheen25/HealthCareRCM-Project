# This script handles Phase 4: Data Cleaning

import pandas as pd
import os

# Base paths — Adjusted to one level up from scripts directory
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
EXTRACT_PATH = os.path.join(BASE_DIR, 'outputs', 'extract_outputs')
CPT_PATH = os.path.join(BASE_DIR, 'cptcodes', 'cptcodes.csv')
DIM_OUTPUT_PATH = os.path.join(BASE_DIR, 'outputs', 'dim_outputs')
FACT_OUTPUT_PATH = os.path.join(BASE_DIR, 'outputs', 'fact_outputs')

os.makedirs(DIM_OUTPUT_PATH, exist_ok=True)
os.makedirs(FACT_OUTPUT_PATH, exist_ok=True)

# Verify paths
print(f"BASE_DIR: {BASE_DIR}")
print(f"EXTRACT_PATH exists? {os.path.exists(EXTRACT_PATH)}")
print(f"CPT_PATH exists? {os.path.exists(CPT_PATH)}")

# -------------------- 1. dim_providers --------------------
provider_file = os.path.join(EXTRACT_PATH, 'providers_extracted.csv')
df_prov = pd.read_csv(provider_file)
print(f"📌 Providers Columns: {df_prov.columns.tolist()}")

df_prov.columns = df_prov.columns.str.strip().str.lower()

required_cols = {'providerid', 'firstname', 'lastname', 'specialization', 'deptid', 'npi'}
if required_cols.issubset(df_prov.columns):
    df_prov['providername'] = df_prov['firstname'].fillna('') + ' ' + df_prov['lastname'].fillna('')
    df_prov['specialty'] = df_prov['specialization']
    df_prov['department'] = df_prov['deptid']

    dim_prov = df_prov[['providerid', 'providername', 'specialty', 'department', 'npi']].copy()
    dim_prov.insert(0, 'provider_key', range(1, len(dim_prov) + 1))

    dim_prov.to_csv(os.path.join(DIM_OUTPUT_PATH, 'dim_providers.csv'), index=False)
    print("✅ dim_providers saved!")
else:
    print("❌ Missing columns in providers data")

# -------------------- 2. dim_procedures --------------------
df_cpt = pd.read_csv(CPT_PATH)
print(f"📌 CPT Codes Columns: {df_cpt.columns.tolist()}")
df_cpt.columns = df_cpt.columns.str.strip().str.lower()

req_cols_cpt = {'cpt codes', 'procedure code descriptions'}
if req_cols_cpt.issubset(df_cpt.columns):
    df_cpt = df_cpt.rename(columns={
        'cpt codes': 'procedure_key',
        'procedure code descriptions': 'description'
    })

    dim_procedures = df_cpt[['procedure_key', 'description']].drop_duplicates()
    dim_procedures.to_csv(os.path.join(DIM_OUTPUT_PATH, 'dim_procedures.csv'), index=False)
    print("✅ dim_procedures saved!")
else:
    print("❌ Missing columns in CPT codes")

# -------------------- 3. fact_transactions --------------------
trans_file = os.path.join(EXTRACT_PATH, 'transactions_extracted.csv')
df_trans = pd.read_csv(trans_file)
print(f"📌 Transactions Columns: {df_trans.columns.tolist()}")

df_trans.columns = df_trans.columns.str.strip().str.lower()

req_cols_trans = {'transactionid', 'patientid', 'providerid', 'procedurecode', 'paiddate', 'paidamount', 'amounttype'}

if req_cols_trans.issubset(df_trans.columns):
    df_trans_cleaned = df_trans.rename(columns={
        'patientid': 'patient_key',
        'providerid': 'provider_key',
        'procedurecode': 'procedure_key',
        'paiddate': 'date',
        'paidamount': 'amount',
        'amounttype': 'payment_status'
    })

    df_trans_cleaned.insert(0, 'transaction_key', range(1, len(df_trans_cleaned) + 1))
    df_trans_cleaned.to_csv(os.path.join(FACT_OUTPUT_PATH, 'fact_transactions.csv'), index=False)
    print("✅ fact_transactions saved!")
else:
    print("❌ Missing required columns in transactions file!")

# -------------------- 4. fact_claims --------------------
claims_file1 = os.path.join(BASE_DIR, 'claims', 'hospital1_claim_data.csv')
claims_file2 = os.path.join(BASE_DIR, 'claims', 'hospital2_claim_data.csv')
df_claims = pd.concat([pd.read_csv(claims_file1), pd.read_csv(claims_file2)], ignore_index=True)
print(f"📌 Claims Columns: {df_claims.columns.tolist()}")

df_claims.columns = df_claims.columns.str.strip().str.lower()

req_cols_claims = {
    'claimid', 'patientid', 'providerid',
    'claimdate', 'claimamount', 'paidamount', 'claimstatus'
}

if req_cols_claims.issubset(df_claims.columns):
    fact_claims = df_claims.rename(columns={
        'patientid': 'patient_key',
        'providerid': 'provider_key',
        'claimdate': 'date_key',
        'claimamount': 'claim_amount',
        'paidamount': 'paid_amount',
        'claimstatus': 'claim_status'
    })[['claimid', 'patient_key', 'provider_key', 'date_key', 'claim_amount', 'paid_amount', 'claim_status']]

    fact_claims.insert(0, 'claim_key', range(1, len(fact_claims) + 1))
    fact_claims.to_csv(os.path.join(FACT_OUTPUT_PATH, 'fact_claims.csv'), index=False)
    print("✅ fact_claims saved!")
else:
    print("❌ Missing columns in claims")

# -------------------- 5. dim_date --------------------
date_collections = pd.concat([
    df_trans['paiddate'],
    df_claims['claimdate']
], ignore_index=True).dropna().unique()

df_dates = pd.DataFrame({'date_value': pd.to_datetime(date_collections)})
df_dates['date_key'] = df_dates['date_value'].dt.strftime('%Y%m%d').astype(int)
df_dates['day'] = df_dates['date_value'].dt.day
df_dates['month'] = df_dates['date_value'].dt.month
df_dates['year'] = df_dates['date_value'].dt.year
df_dates['quarter'] = df_dates['date_value'].dt.quarter
df_dates['day_name'] = df_dates['date_value'].dt.day_name()
df_dates['month_name'] = df_dates['date_value'].dt.month_name()

dim_date = df_dates[['date_key', 'date_value', 'day', 'month', 'year', 'quarter', 'day_name', 'month_name']].sort_values('date_value')
dim_date.to_csv(os.path.join(DIM_OUTPUT_PATH, 'dim_date.csv'), index=False)
print("✅ dim_date saved!")

#---------------------------dim_patients.csv------------

# -------------------- 6. dim_patients --------------------
# -------------------- 6. dim_patients --------------------
from datetime import datetime

print("\n🧪 Processing dim_patients...")

# Define transformed directory for cleaned input
transformed_dir = os.path.join(BASE_DIR, "outputs", "transformed_outputs")

# Load cleaned patients data
df = pd.read_csv(os.path.join(transformed_dir, "cleaned_patients.csv"))

# Standardize column names
df.columns = df.columns.str.strip().str.lower()

# Rename columns for consistency
df.rename(columns={
    "patientid": "PatientID",
    "firstname": "FirstName",
    "lastname": "LastName",
    "middlename": "MiddleName",
    "ssn": "SSN",
    "phonenumber": "PhoneNumber",
    "gender": "Gender",
    "dateofbirth": "DateOfBirth",
    "address": "Address",
    "modifieddate": "ModifiedDate",
    "insurance": "Insurance"
}, inplace=True)

# 🔍 OPTION B: Print rows that would be dropped due to missing fields
required_fields = ["PatientID", "FirstName", "LastName", "Gender", "DateOfBirth", "Insurance"]
missing_df = df[df[required_fields].isnull().any(axis=1)]
print(f"⚠️ Rows with missing required fields: {len(missing_df)}")
if not missing_df.empty:
    print(missing_df[required_fields].head(5))  # Show only first 5 problematic rows

# 🔧 OPTION A: Handle missing values gracefully before drop
df["FirstName"] = df["FirstName"].fillna("Unknown")
df["LastName"] = df["LastName"].fillna("Unknown")
df["Gender"] = df["Gender"].fillna("Other")
df["DateOfBirth"] = pd.to_datetime(df["DateOfBirth"], errors="coerce")  # convert and flag bad dates

# Drop only rows where ESSENTIAL fields are missing
before = len(df)
df = df.dropna(subset=["PatientID", "DateOfBirth"])
after = len(df)
print(f"✅ Dropped {before - after} rows with missing critical fields. Remaining: {after}")

# Create dim_patients with selected fields
dim_patients = df[["PatientID", "FirstName", "LastName", "Gender", "DateOfBirth"]].copy()

# Add SCD columns
dim_patients["effective_date"] = pd.to_datetime(datetime.today().date())
dim_patients["expiry_date"] = "9999-12-31"
dim_patients["is_current"] = True
dim_patients["version"] = 1

# Add surrogate key
dim_patients.insert(0, "patient_key", range(1, len(dim_patients) + 1))

# Save to dim_outputs
dim_patient_path = os.path.join(DIM_OUTPUT_PATH, "dim_patients.csv")
print(f"✅ dim_patients saved with {len(dim_patients)} records.")
dim_patients.to_csv(dim_patient_path, index=False)
print(f"✅ dim_patients saved to: {dim_patient_path}")
print(f"🧾 Total records: {len(dim_patients)}")
