import os
def save_to_csv(df, city:str) -> None:
    # Save to CSV
    csv_file = "full_weather_data.csv"
    write_header = not os.path.exists(csv_file)
    df.to_csv(csv_file, mode = "a", index=False, header = write_header)
    print("Weather data added to CSV!")