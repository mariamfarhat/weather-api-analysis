import requests
import csv
import pandas as pd
import os
import dotenv 
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---

API_KEY = os.getenv('OPENWEATHER_API_KEY')
BASE_URL = 'http://api.openweathermap.org/data/2.5/forecast'

if not API_KEY:
    print("Error: API_KEY not found in .env file!")
    exit()

# --- Fetch Weather Data ---
city = input("Enter a city name: ")
request_url = f"{BASE_URL}?appid={API_KEY}&q={city}&units=metric"
response = requests.get(request_url)

# Check if the request was successful 
if response.status_code == 200:
    data = response.json()
    if "list" not in data:
        print("API returned an error!")
        print(data)
    else:
        df = pd.json_normalize(data["list"])
        # Add city column
        df['city'] = city
        # Save to CSV
        csv_file = "full_weather_data.csv"
        write_header = not os.path.exists(csv_file)
        df.to_csv(csv_file, mode = "a", index=False, header = write_header)
        print("Weather data added to CSV!")
        
else:
    print(f"Error fetching data: HTTP {response.status_code}")
    print(response.text)
