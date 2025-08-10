# this has the ETL Pipelining

import os
import pandas as pd
from transform_logic import clean_patient_data, clean_claim_data

# ✅ Provide full paths to your raw input files here
PATIENTS_FILE = r"C:\Users\hp\OneDrive\Desktop\rcm_project\outputs\extract_outputs\patients_extracted.csv"
CLAIMS_FILE_1 = r"C:\Users\hp\OneDrive\Desktop\rcm_project\outputs\extract_outputs\hospital1_claim_data_extracted.csv"
CLAIMS_FILE_2 = r"C:\Users\hp\OneDrive\Desktop\rcm_project\outputs\extract_outputs\hospital2_claim_data_extracted.csv"

OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../outputs/transformed_outputs")
os.makedirs(OUTPUT_PATH, exist_ok=True)

# ------------------------------
#  Transform Patients
# ------------------------------
def process_patients():
    if not os.path.exists(PATIENTS_FILE):
        print(f" Missing input: {PATIENTS_FILE}")
        return

    print("🔄 Loading patient data...")
    df_patients = pd.read_csv(PATIENTS_FILE)

    print(" Cleaning patient data...")
    cleaned_patients = clean_patient_data(df_patients)

    output_path = os.path.join(OUTPUT_PATH, "cleaned_patients.csv")
    cleaned_patients.to_csv(output_path, index=False)
    print(f"✅ Cleaned patients saved to: {output_path}")
    print(f" Rows: {len(cleaned_patients)}")


# ------------------------------
#  Transform Claims
# ------------------------------
def process_claims():
    all_claims = []

    for path in [CLAIMS_FILE_1, CLAIMS_FILE_2]:
        if not os.path.exists(path):
            print(f" Missing: {path}")
            continue
        print(f"🔄 Loading {path}")
        df = pd.read_csv(path)
        cleaned = clean_claim_data(df, source=os.path.basename(path))
        all_claims.append(cleaned)

    if not all_claims:
        print(" No claim data found.")
        return

    combined_claims = pd.concat(all_claims, ignore_index=True)
    output_path = os.path.join(OUTPUT_PATH, "cleaned_claims.csv")
    combined_claims.to_csv(output_path, index=False)
    print(f"✅ Cleaned claims saved to: {output_path}")
    print(f" Rows: {len(combined_claims)}")


# ------------------------------
# Main Trigger
# ------------------------------
if __name__ == "__main__":
    print(" Starting ETL Process for Phase 3...")
    process_patients()
    process_claims()
    print(" 🚀 ETL Transformation Completed,")
