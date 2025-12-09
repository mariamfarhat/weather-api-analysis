import os
from dotenv import load_dotenv
import sqlalchemy
import urllib

load_dotenv()

def get_db_connection():
    print("🔍 Starting connection function")

    DB_SERVER = os.getenv("DB_SERVER")
    DB_DATABASE = os.getenv("DB_DATABASE")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_DRIVER = os.getenv("DB_DRIVER")

    print("Env variables loaded:")
    print(DB_SERVER, DB_DATABASE, DB_USERNAME, DB_DRIVER)

    if not all([DB_SERVER, DB_DATABASE, DB_USERNAME, DB_PASSWORD, DB_DRIVER]):
        print("❌ One or more .env variables are missing!")
        return None

    try:
        connection_string = urllib.parse.quote_plus(
            f"DRIVER={DB_DRIVER};SERVER={DB_SERVER};DATABASE={DB_DATABASE};UID={DB_USERNAME};PWD={DB_PASSWORD}"
        )
        print("Connection string built.")

        engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}")
        print("Engine created. Testing connection...")

        with engine.connect() as conn:
            print("✔ Connection successful!")

        return engine

    except Exception as e:
        print("❌ Connection failed:", e)
        return None

# --- Test ---
engine = get_db_connection()
print("Returned engine:", engine)
