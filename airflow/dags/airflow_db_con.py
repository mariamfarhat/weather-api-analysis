from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.sdk.bases.hook import BaseHook
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
import logging

logger = logging.getLogger(__name__)

def get_mssql_connection(conn_id: str ='mssql_weather'):
    logger.info(f"Connecting to MSSQL with connection ID: {conn_id}")
    try: 
        conn = BaseHook.get_connection('mssql_weather')
        logger.info(f"Loaded Airflow connection: {conn.host}:{conn.port}")
        # Extract extras (driver)
        extras = conn.extra_dejson or {}
        driver = extras.get("driver", "ODBC Driver 17 for SQL Server")

        user = conn.login
        password = conn.password
        host = conn.host
        port = conn.port or 1433
        database = conn.schema

        # Build connection string safely
        driver_enc = quote_plus(driver)
        password_enc = quote_plus(password)

        uri = (
            f"mssql+pyodbc://{user}:{password_enc}"
            f"@{host}:{port}/{database}?driver={driver_enc}"
        )

        logger.info(f"Creating SQLAlchemy engine (host={host}, db={database}, driver='{driver}')")

        # Create engine
        engine = create_engine(
            uri,
            fast_executemany=True,
            connect_args={"timeout": 30}
        )

        # Optional: test connection
        try:
            with engine.connect() as conn_test:
                conn_test.execute(text("SELECT 1")) 
            logger.info("MSSQL connection test succeeded.")
        except Exception as test_err:
            logger.error("Connection test failed inside engine.connect()", exc_info=True)
            raise test_err
        return engine

    except SQLAlchemyError as db_err:
        logger.error("SQLAlchemy error during MSSQL engine creation", exc_info=True)
        raise

    except Exception as e:
        logger.error(f"Unexpected error while creating MSSQL engine: {str(e)}", exc_info=True)
        raise