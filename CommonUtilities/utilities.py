import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from Config.config import *
import pytest
import logging

# Create mysql engine
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

# Create Oracle engine
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

# Logging mechanism
logging.basicConfig(
    filename='Logs/etlprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)

# verify data between file as source and database as target
def file_to_db_verify(file_path,file_type,table_name,db_engine):
    if file_type == 'csv':
        df_expected = pd.read_csv(file_path)
    elif file_type == 'xml':
        df_expected = pd.read_xml(file_path, xpath='.//item')
    elif file_type == 'json':
        df_expected = pd.read_json(file_path)
    else:
        raise ValueError(f"Unsupported file type passed {file_type}")

    query = f"select * from {table_name}"
    df_actual = pd.read_sql(query, db_engine)
    assert df_actual.equals(df_expected),f"Data extraction failed to load in {table_name}"

