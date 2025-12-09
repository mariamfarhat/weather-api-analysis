import os
import requests
import pandas as pd
import json
from pathlib import Path
from dotenv import load_dotenv

# Adjust the path based on your project structure
project_root = Path(__file__).parent.parent.parent  # Go up 3 levels from DAG
env_path = project_root / "src" / "extract_load" / ".env"
load_dotenv(env_path)
def fetch_weather(city: str) -> pd.DataFrame:

    #Base URL and API key
    API_KEY = os.getenv('OPENWEATHER_API_KEY')
    BASE_URL = 'http://api.openweathermap.org/data/2.5/forecast'

    if not API_KEY:
        print("Error: API_KEY not found in .env file!")
        exit()  
    #Takes a city name as string input. Returns a pandas DataFrame with the weather data.#
    # --- Fetch Weather Data for a given city ---
    request_url = f"{BASE_URL}?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(request_url)
    response.raise_for_status()

    # Check if the request was successful 
    data = response.json()
    if "list" not in data:
        raise ValueError("Unexpected API response structure for {city}: 'list' key not found.")
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
    return df