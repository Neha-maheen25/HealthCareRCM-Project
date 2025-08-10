import pandas as pd
import numpy as np
import re
from datetime import datetime

def is_valid_email(email):
    return bool(re.match(r"[^@]+@[^@]+\.[^@]+", str(email)))

def clean_phone_number(phone):
    if pd.isna(phone):
        return np.nan
    digits = re.sub(r'\D', '', str(phone))
    return digits if len(digits) >= 10 else np.nan

def calculate_age(dob):
    if pd.isnull(dob):
        return np.nan
    today = pd.to_datetime('today')
    return int((today - dob).days / 365.25)

def clean_patient_data(df, source=None):
    # Standardize column names
    df.columns = df.columns.str.strip().str.lower()

    rename_map = {
        "id": "patientid",
        "f_name": "firstname",
        "l_name": "lastname",
        "m_name": "middlename",
        "dob": "dateofbirth"
    }
    df.rename(columns=rename_map, inplace=True)

    expected_cols = [
        "patientid", "firstname", "lastname", "middlename", "ssn",
        "phonenumber", "gender", "dateofbirth", "address",
        "modifieddate", "insurance", "email"
    ]

    for col in expected_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Clean names to proper case
    df['firstname'] = df['firstname'].astype(str).str.title()
    df['lastname'] = df['lastname'].astype(str).str.title()
    df['middlename'] = df['middlename'].astype(str).str.title()

    # Clean phone numbers
    df['phonenumber'] = df['phonenumber'].apply(clean_phone_number)

    # Validate emails
    df['email_valid'] = df['email'].apply(is_valid_email)

    # Flag bad records
    df['data_quality_flag'] = df.apply(
        lambda row: 'Invalid Email' if not row['email_valid'] else '',
        axis=1
    )

    # Convert date and calculate age
    df['dateofbirth'] = pd.to_datetime(df['dateofbirth'], errors='coerce')
    df['age'] = df['dateofbirth'].apply(calculate_age)

    # Time dimensions
    df['modifieddate'] = pd.to_datetime(df['modifieddate'], errors='coerce')
    df['year'] = df['modifieddate'].dt.year
    df['month'] = df['modifieddate'].dt.month
    df['quarter'] = df['modifieddate'].dt.quarter
    df['dayofweek'] = df['modifieddate'].dt.day_name()

    # Drop duplicates
    df.drop_duplicates(subset=["patientid"], inplace=True)

    return df


def clean_claim_data(df: pd.DataFrame, source: str = "") -> pd.DataFrame:
    df = df.copy()

    # Standardize column names
    df.columns = [col.lower().strip() for col in df.columns]

    # Rename columns
    rename_map = {
        "claimamount": "claim_amount",
        "paidamount": "paid_amount",
        "claimstatus": "claim_status",
        "claimdate": "claim_date",
    }
    df.rename(columns=rename_map, inplace=True)

    # Fill missing columns
    for col in ["claim_amount", "paid_amount", "claim_status", "insurance_coverage_pct"]:
        if col not in df.columns:
            print(f" Missing column '{col}' in {source}, filling with None")
            df[col] = None

    # Calculate insurance percentage
    df["insurance_coverage_pct"] = np.where(
        (df["claim_amount"].notnull()) & (df["paid_amount"].notnull()) & (df["claim_amount"] != 0),
        (df["paid_amount"] / df["claim_amount"]) * 100,
        np.nan
    )

    # Categorize payment status
    def categorize_status(row):
        if pd.isnull(row['paid_amount']):
            return "Pending"
        elif row['paid_amount'] == 0:
            return "Denied"
        elif row['paid_amount'] < row['claim_amount']:
            return "Partial"
        else:
            return "Paid"

    df['payment_category'] = df.apply(categorize_status, axis=1)

    # Time breakdown
    df['claim_date'] = pd.to_datetime(df['claim_date'], errors='coerce')
    df['year'] = df['claim_date'].dt.year
    df['month'] = df['claim_date'].dt.month
    df['quarter'] = df['claim_date'].dt.quarter
    df['weekday'] = df['claim_date'].dt.day_name()

    # Select columns
    final_cols = [
        "claimid", "patientid", "providerid", "claim_amount", "paid_amount",
        "claim_status", "payment_category", "insurance_coverage_pct", "claim_date",
        "year", "month", "quarter", "weekday"
    ]
    df = df[[col for col in final_cols if col in df.columns]]

    return df
