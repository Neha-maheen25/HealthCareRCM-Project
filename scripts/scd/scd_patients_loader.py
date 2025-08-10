# scripts/scd/scd_patients_loader.py

import pandas as pd
import mysql.connector
from datetime import datetime
import os

def connect_mysql():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password#neha",
        database="hospital_star"
    )

def get_existing_dim_patients(conn):
    query = "SELECT * FROM dim_patients WHERE is_current = 1"
    return pd.read_sql(query, conn)

def scd_type2_apply(new_df, existing_df):
    scd_columns = ['firstname', 'lastname', 'gender', 'dateofbirth', 'insurance']
    new_records = []

    for _, new_row in new_df.iterrows():
        existing_rows = existing_df[existing_df['patientid'] == new_row['patientid']]
        
        if existing_rows.empty:
            new_row_dict = {
                **new_row,
                'effective_date': datetime.now().date(),
                'expiry_date': None,
                'is_current': 1,
                'version': 1
            }
            new_records.append(new_row_dict)
        else:
            current = existing_rows.iloc[0]
            has_changed = any(new_row[col] != current[col] for col in scd_columns)
            
            if has_changed:
                # Close old record
                existing_df.loc[existing_df['patientid'] == new_row['patientid'], 'expiry_date'] = datetime.now().date()
                existing_df.loc[existing_df['patientid'] == new_row['patientid'], 'is_current'] = 0

                new_version = current['version'] + 1
                new_row_dict = {
                    **new_row,
                    'effective_date': datetime.now().date(),
                    'expiry_date': None,
                    'is_current': 1,
                    'version': new_version
                }
                new_records.append(new_row_dict)

    new_records_df = pd.DataFrame(new_records)
    final_df = pd.concat([existing_df, new_records_df], ignore_index=True)
    return final_df

def main():
    print("🔁 Starting SCD Type 2 load for dim_patients...")

    # Load cleaned data
    cleaned_path = "outputs/transformed_outputs/cleaned_patients.csv"
    new_df = pd.read_csv(cleaned_path)
    new_df = new_df[['patientid', 'firstname', 'lastname', 'gender', 'dateofbirth', 'insurance']]
    
    # Connect to MySQL
    conn = connect_mysql()
    existing_df = get_existing_dim_patients(conn)

    # Apply SCD2
    final_df = scd_type2_apply(new_df, existing_df)

    # Overwrite MySQL dim_patients
    cursor = conn.cursor()
    cursor.execute("DELETE FROM dim_patients")
    conn.commit()

    for _, row in final_df.iterrows():
        cursor.execute("""
            INSERT INTO dim_patients (patientid, firstname, lastname, gender, dateofbirth, insurance,
                                      effective_date, expiry_date, is_current, version)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            row['patientid'], row['firstname'], row['lastname'], row['gender'], row['dateofbirth'],
            row['insurance'], row['effective_date'], row['expiry_date'], row['is_current'], row['version']
        ))

    conn.commit()
    cursor.close()
    conn.close()

    # Save to CSV
    output_path = "outputs/dim_outputs/dim_patients.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_df.to_csv(output_path, index=False)
    print(f"✅ dim_patients.csv written with {len(final_df)} records")

if __name__ == "__main__":
    main()
