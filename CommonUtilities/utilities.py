import os.path
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging

from Config.config import MYSQL_PASSWORD, MYSQL_USER, MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, ORACLE_USER, \
    ORACLE_PASSWORD, ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE

# Create MySQL engine
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

# Create Oracle engine
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/?service_name={ORACLE_SERVICE}')

# Ensure Logs directory exists
if not os.path.exists('Logs'):
    os.makedirs('Logs')

# Logging mechanism
logging.basicConfig(
    filename='Logs/etlprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levellevel)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)

def write_differences_to_csv(df_expected, df_actual, file_name):
    # Align DataFrames
    df_expected = df_expected.reindex(sorted(df_expected.columns), axis=1)
    df_actual = df_actual.reindex(sorted(df_actual.columns), axis=1)

    # Find differences
    differences = pd.concat([df_expected, df_actual]).drop_duplicates(keep=False)

    # Write differences to CSV
    if not differences.empty:
        differences.to_csv(file_name, index=False)
        logger.info(f"Differences written to {file_name}")
    else:
        logger.info(f"No differences found for {file_name}")

# Verify data between file as source and database as target
def file_to_db_verify(file_path, file_type, table_name, db_engine, file_name):
    if file_type == 'csv':
        df_expected = pd.read_csv(file_path)
    elif file_type == 'xml':
        df_expected = pd.read_xml(file_path, xpath='.//item')
    elif file_type == 'json':
        df_expected = pd.read_json(file_path)
    else:
        raise ValueError(f"Unsupported file type passed {file_type}")

    logger.info(f"Expected data is :\n{df_expected}")

    query = f"SELECT * FROM {table_name}"
    df_actual = pd.read_sql(query, db_engine)

    logger.info(f"Actual data is :\n{df_actual}")

    # Write differences to CSV
    write_differences_to_csv(df_expected, df_actual, file_name)

    # Assert that actual data matches expected data
    assert df_actual.equals(df_expected), f"Data comparison failed to load in {table_name}"

# Verify data between two databases or two tables
def db_to_db_verify(query1, db_engine1, query2, db_engine2, file_name):
    df_expected = pd.read_sql(query1, db_engine1)
    logger.info(f"Expected data is :\n{df_expected}")

    df_actual = pd.read_sql(query2, db_engine2)
    logger.info(f"Actual data is :\n{df_actual}")

    # Write differences to CSV
    write_differences_to_csv(df_expected, df_actual, file_name)

    # Assert that actual data matches expected data
    assert df_actual.astype(str).equals(df_expected.astype(str)), f"Data comparison failed to load"

# Function to check if a given file exists in the given location
def check_file_exists(file_path):
    return os.path.isfile(file_path)

# Function to check if a given file is not a zero-byte file (empty file)
def check_file_size(file_path):
    return os.path.getsize(file_path) != 0
