import pandas as pd
import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Password#neha",
    database="hospital_star"
)

df = pd.read_sql("SELECT * FROM dim_patients", conn)
df.to_csv("outputs/dim_outputs/dim_patients.csv", index=False)
conn.close()
print("✅ dim_patients.csv exported successfully.")
