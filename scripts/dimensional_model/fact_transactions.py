import pandas as pd

df = pd.read_csv("outputs/fact_outputs/fact_transactions.csv")

# Print column names
print("Columns before fix:", df.columns.tolist())

# Fix duplicate column names manually or automatically
df = df.loc[:, ~df.columns.duplicated()]  # removes duplicated columns

# OR if needed, rename:
# df.columns = ['transactionid', 'patientid', 'amounttype', 'paidamount', 'procedurecode', 'providerid']  # example

# Save back the cleaned CSV
df.to_csv("outputs/fact_outputs/fact_transactions.csv", index=False)

print("✅ Fixed and saved cleaned fact_transactions.csv")
