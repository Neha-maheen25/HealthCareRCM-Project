import pandas as pd
import os

print("=== Testing CSV Files ===")
try:
    csv_path = r'C:\Users\hp\OneDrive\Desktop\rcm_project\claims\hospital1_claim_data.csv'
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        print(f"✅ CSV loaded successfully with {len(df)} records")
    else:
        print(f"❌ File not found at: {csv_path}")
except Exception as e:
    print(f"❌ CSV read failed: {e}")