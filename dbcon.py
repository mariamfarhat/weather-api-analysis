import os
from dotenv import load_dotenv
import pyodbc

load_dotenv()


def get_db_connection():
    ### --- Read variables from .env file ---
    DB_SERVER = os.getenv("DB_SERVER")
    DB_DATABASE = os.getenv("DB_DATABASE")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_DRIVER = os.getenv("DB_DRIVER")
    
   ###--- Check if all required variables are present ---###
    if not all([DB_SERVER, DB_DATABASE, DB_USERNAME, DB_PASSWORD, DB_DRIVER]):
        print("Error: One or more database configuration variables are missing in the .env file!")
        exit()


    ### --- Build the MSSQL Connection String --- ###
    connection_string = (
        f"DRIVER={DB_DRIVER};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_DATABASE};"
        f"UID={DB_USERNAME};"   
    )

    ### --- Establish the Database Connection --- ###
    try:
        conn = pyodbc.connect(connection_string, password=DB_PASSWORD)
        print("Database connection established successfully.")
        return conn
    except pyodbc.Error as e:
        print("Error connecting to database:", e)
        exit()  