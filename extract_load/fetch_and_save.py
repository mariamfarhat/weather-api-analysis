import requests
import csv
import pandas as pd
import os
import dotenv 
from dotenv import load_dotenv
from dbcon import get_db_connection
import json

# Load environment variables from .env file
load_dotenv()

# Get the engine from the connectionn file
engine = get_db_connection()
def main():
    # --- Configuration ---

    API_KEY = os.getenv('OPENWEATHER_API_KEY')
    BASE_URL = 'http://api.openweathermap.org/data/2.5/forecast'

    if not API_KEY:
        print("Error: API_KEY not found in .env file!")
        exit()

    # --- Fetch Weather Data ---
    city = "London"
    request_url = f"{BASE_URL}?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(request_url)
    response.raise_for_status()

    # Check if the request was successful 
    data = response.json()
    if "list" not in data:
        raise ValueError("Unexpected API response structure: 'list' key not found.")
    else:
        df = pd.json_normalize(data["list"])
    
    # Extract nested fields #
    df['weather_main'] = df["weather"].apply(lambda x: x[0]["main"] if isinstance(x, list) and len(x) > 0 else None)

    df['weather_description'] = df["weather"].apply(lambda x: x[0]["description"] if isinstance(x, list) and len(x) > 0 else None)

    #Convert list/dict columns to json
    df = df.apply(lambda col: col.map(
    lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x))
    # Add city column
    df['city'] = city
    # Save to CSV
    csv_file = "full_weather_data.csv"
    write_header = not os.path.exists(csv_file)
    df.to_csv(csv_file, mode = "a", index=False, header = write_header)
    print("Weather data added to CSV!")
    # Add to databse
    df.to_sql(
    'raw_data', 
    engine, 
    if_exists='append',  # append new rows
    index=False
)
print("Weather data inserted into SQL Server!")


if __name__ == "__main__":
    main()