import os
import sys
from airflow.sdk import dag, task
from datetime import timedelta
import pendulum
from airflow_dbt_python.operators.dbt import DbtRunOperator

# Add project root to Python path
#project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
#sys.path.insert(0, project_root)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
dbt_project_path = os.path.join(project_root, "dbt", "weather_data")
from extract_weather_api import fetch_weather
from load_to_csv_file import save_to_csv
from load_to_sql import insert_df_into_db

def extract_weather_data(city: str):
    return fetch_weather(city)

def load_data_to_csv(df, city: str):
    save_to_csv(df, city)

def load_data_to_sql(df):
    insert_df_into_db(df, table_name='raw_data', conn_id='mssql_weather')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='weather_data_pipeline',
    default_args=default_args,
    description='A simple weather data pipeline',
    schedule='@hourly',
    start_date=pendulum.datetime(2025, 12, 4, tz="UTC"),
    catchup=False,
    tags=['weather', 'data_pipeline'],
)
def weather_data_pipeline():


    # Task to run all dbt models in your project
    dbt_run_models = DbtRunOperator(
        task_id="run_dbt_models_task",
        project_dir=dbt_project_path,
        profiles_dir=dbt_project_path,
        profile="weather_data",  # Matches the name in profiles.yml
        target="dev",
    )

    @task(task_id='city_names_list')
    def get_city_names():
        return ['Beirut', 'Los Angeles', 'London', 'Paris'] 

    @task(task_id='extract_weather_data')
    def extract_weather(city:str):
        print(f"Extracting weather data for {city}")
        results = extract_weather_data(city)
        return results
    
    @task(task_id='load_to_csv')
    def load_to_csv(df, city: str):
        return load_data_to_csv(df, city)   

    @task(task_id='load_to_sql')
    def load_to_sql(df):
        return load_data_to_sql(df)

    # get city names
    cities = get_city_names()
    weather_results= extract_weather.expand(city=cities)
    load_to_csv.expand(df=weather_results, city=cities)
    sql_results = load_to_sql.expand(df=weather_results)

    sql_results >> dbt_run_models

weather_data_pipeline()