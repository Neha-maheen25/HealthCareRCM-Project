#this script is used for for testing and internal query during the process consider it as testing script
import pandas as pd
df = pd.read_csv(r"C:\Users\hp\OneDrive\Desktop\rcm_project\outputs\transformed_outputs\cleaned_patients.csv")
print(df.columns.tolist())

