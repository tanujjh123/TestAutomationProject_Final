import os.path
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
    logger.info(f"expected data is :{df_expected}")
    query = f"select * from {table_name}"
    df_actual = pd.read_sql(query, db_engine)
    logger.info(f"actual data is :{df_actual}")
    #implement the logic to write the differential data between source and target
    assert df_actual.equals(df_expected),f"Data comparision failed to load in {table_name}"


# verify data between two databases or 2 tables
def db_to_db_verify(query1,db_engine1,query2,db_engine2):
    df_expected  = pd.read_sql(query1,db_engine1)
    logger.info(f"expected data is :{df_expected}")
    df_actual = pd.read_sql(query2, db_engine2)
    logger.info(f"actual data is :{df_actual}")
    # implement the logic to write the differential data between source and target
    assert df_actual.astype(str).equals(df_expected.astype(str)), f"Data comparision failed to load"

# Function to check if a given file exists in the given location
def check_file_exists(file_path):
    if(os.path.isfile(file_path) == True):
        return True
    else:
        return False

# Function to check if a given file is not a zero byte file ( empty file )
def check_file_size(file_path):
    if(os.path.getsize(file_path) != 0):
        return True
    else:
        return False

