# scripts/test_connection.py
import mysql.connector

print("=== Testing MySQL Connection ===")
try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Password#neha"  # Use your actual password
    )
    print("✅ MySQL connection successful!")
    conn.close()
except Exception as e:
    print(f"❌ MySQL connection failed: {e}")