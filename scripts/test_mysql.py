import mysql.connector

def test_connection(db_name):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Password#neha", 
            database=db_name
        )
        print(f" Successfully connected to {db_name}")
        conn.close()
    except Exception as e:
        print(f" Connection failed for {db_name}: {e}")

if __name__ == "__main__":
    test_connection("hospital_a_db")
    test_connection("hospital_b_db")

