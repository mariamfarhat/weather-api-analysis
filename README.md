# 🌦️ Weather Data Pipeline
## **🛰️ Project Overview**

This project fetches weather data for four cities from an external API, stores it in CSV and MSSQL, and runs dbt models, all managed by Airflow 3.

### **⚙️ Features**

- **Extract** weather data from a public weather API
- **Load into** Store raw data into a CSV file and Microsoft SQL Server (MSSQL) database.
- **Transform** Run dbt models to clean and aggregate the weather data
- **Orchestrate** Schedule and monitor the entire workflow in Airflow

## **🏗️ Pipeline Architecture**
**1. Weather Data Extraction**
A Python script sends requests to the weather API and returns normalized JSON → Pandas DataFrame.

**2. Loading**
- Save raw weather data into /data/weather_raw.csv
- Insert the same data into a SQL Server table using SQLAlchemy or pyodbc

**3. Transformation with dbt**
- Airflow triggers dbt via dbtRunOperator (Airflow 3)
- dbt builds staging and analytics tables
- Produces cleaned, ready-to-query datasets

**4. Orchestration with Airflow**
- Manages task dependencies
- Handles retries, logging, and scheduling

## **⚙️ Technologies Used**
- Python 3.x
- Apache Airflow 3.x
- Microsoft SQL Server (MSSQL)
- dbt-core (+ dbt-sqlserver/dbt-mssql)
- Pandas
- Weather API (e.g., OpenWeatherMap)
- SQLAlchemy / pyodbc

## 🔧 Installation and Setup:
**1. Clone the repo**

`git clone https://github.com/mariamfarhat/weather-api-analysis`

`cd weather-api-analysis`

**2. Create a virtual environment**

`python -m venv .venv`

`source .venv/bin/activate`

**3. Install dependencies**

`pip install -r requirements.txt`

**4. Create your .env file**

Inside the extract_load folder inside the src folder.

OPENWEATHER_API_KEY=your_api_key

UNITS=metric

DB_SERVER=ip_address,port

DB_DATABASE=your_db_name

DB_USERNAME=your_db_username

DB_PASSWORD=your_db_password

DB_DRIVER=ODBC Driver 17 for SQL Server


**5. Configure dbt**

In profiles.yml, add SQL Server target settings.

**6. Start Airflow**

`airflow standalone`

## **🚀 How to Run the Pipeline**
1- Open the Airflow UI at: http://localhost:8080

2- Locate the DAG: weather_pipeline

3- Switch it ON

4- Trigger a manual run

5- Airflow will:

    - Fetch data from API
    
    - Save CSV
    
    - Insert into MSSQL
    
    - Run dbt transformations
    
    - Log everything in the UI

## **📊 dbt Models**
### Staging Layer
stg_daily_weather_city.sql
- Standardizing column names.
- Converting data typer and units.
- Handling missing values
Note: This model is built as a SQL View, meaning it executes the query against the raw data every time it's referenced, ensuring the data is always up-to-date without storing a duplicate table.

### Intermediate Layer
int_daily_day_weather_description.sql
- Materialization: View
- Source: stg_daily_weather_city.sql
- Daytime Filtering
- Weather Day Description Summary
- Numerical Aggregations

int_daily_night_weather_description.sql
- Materialization: View
- Source: stg_daily_weather_city.sql
- Nightime Filtering
- Weather Night Description Summary
- Numerical Aggregations

### Analytics Layer
primary_weather_forecast.sql
- Aggregates weather per city
- Calculates min/max/avg temperature
- Prepares data for BI dashboards

## **📈 Future Enhancements
- Add more cities dynamically from a config file.
- Add incremental dbt models
- Improve error alerting (Slack, email)
