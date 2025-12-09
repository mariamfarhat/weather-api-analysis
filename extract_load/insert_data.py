from dbcon import get_db_connection

try:
    conn = get_db_connection()
    cursor = conn.cursor()
    print("✔ Connected successfully!")

except Exception as e:
    print("❌ Error:", e)

finally:
    conn.close()
