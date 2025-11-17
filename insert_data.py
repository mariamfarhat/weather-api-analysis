from dbcon import get_db_connection

try:
    conn = get_db_connection()
    cursor = conn.cursor()
    print("✔ Connected successfully!")

    # Test query: list all tables in the database
    cursor.execute(f"SELECT * FROM EMPLOYEE")

    rows = cursor.fetchall()

    if not rows:
        print("⚠ No data found in the table.")
    else:
        # Print each row
        for row in rows:
            print(row)

except Exception as e:
    print("❌ Error:", e)

finally:
    conn.close()
