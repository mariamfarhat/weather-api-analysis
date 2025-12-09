from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from pathlib import Path
from dotenv import load_dotenv
import sys
import os
from datetime import datetime
import pandas as pd
import logging 
from airflow_db_con import get_mssql_connection
# Add the path to the project root first
current_dir = os.path.dirname(os.path.abspath(__file__)) #Gives the folder where the current Python file is located.
project_root = os.path.abspath(os.path.join(current_dir, '../..')) # Gives the path to the project root by going up two levels from the current directory.
sys.path.insert(0, project_root) # Insert the project root path at the start of sys.path
# Now import your module
from src.extract_load.dbcon import get_db_connection
# Load environment variables
project_root_path = Path(project_root)
env_path = project_root_path / "src" / "extract_load" / ".env"

load_dotenv(env_path)

logger = logging.getLogger(__name__)

def insert_df_into_db(df: pd.DataFrame, table_name: str = 'raw_data', conn_id ='mssql_weather') -> None:
    # Input validation
    if df is None:
        raise ValueError("DataFrame cannot be None")
        
    if df.empty:
        logger.warning("DataFrame is empty, nothing to insert")
        return 0
    engine = None

    try:        
        # Get database connection
        logger.info("Establishing database connection...")
        engine = get_mssql_connection(conn_id)
        if engine is None:
            raise ConnectionError("Failed to establish database connection")
        
        with engine.begin() as conn:
            df.to_sql(
                table_name, 
                conn, 
                if_exists='append',  # append new rows
                index=False,
                method='multi',  # Use multi-row insert
                chunksize=1000  # Number of rows per batch insert
            )   
        logger.info(f"Inserted {len(df)} records into {table_name} table.")
        
    except Exception as e:
        logger.error(f"Database insertion failed!")
        logger.error(f"Error: {str(e)}")
        
        # Log DataFrame info for debugging
        if df is not None:
            print(f"Failed DataFrame shape: {df.shape}")
            print(f"Failed DataFrame columns: {list(df.columns)}") 
        # Re-raise the exception so Airflow knows the task failed
            raise
        
    finally:
        # Always close the connection
        if engine:
            engine.dispose()
            logger.info("Database connection closed.")