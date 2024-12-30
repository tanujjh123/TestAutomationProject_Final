import os.path
import pandas as pd
from sqlalchemy import create_engine
import pytest
import logging

from Config.config import *

# Create MySQL engine
mysql_engine = create_engine(
    f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

# Create Oracle engine
oracle_engine = create_engine(
    f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

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


def write_differences_to_csv(df_expected, df_actual, file_name='Logs/differences.csv'):
    # Align DataFrames
    df_expected = df_expected.reindex(sorted(df_expected.columns), axis=1)
    df_actual = df_actual.reindex(sorted(df_actual.columns), axis=1)

    # Find differences
    df_diff = df_expected.merge(df_actual, indicator=True, how='outer')
    df_diff = df_diff[df_diff['_merge'] != 'both']

    # Write differences to CSV
    if not df_diff.empty:
        df_diff.to_csv(file_name, index=False)
        logger.info(f"Differences written to {file_name}")
    else:
        logger.info("No differences found.")


def file_to_db_verify(file_path, file_type, table_name, db_engine):
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
    write_differences_to_csv(df_expected, df_actual)

    # Assert that actual data matches expected data
    assert df_actual.equals(df_expected), f"Data comparison failed to load in {table_name}"


def db_to_db_verify(query1, db_engine1, query2, db_engine2):
    df_expected = pd.read_sql(query1, db_engine1)
    logger.info(f"Expected data is :\n{df_expected}")

    df_actual = pd.read_sql(query2, db_engine2)
    logger.info(f"Actual data is :\n{df_actual}")

    # Write differences to CSV
    write_differences_to_csv(df_expected, df_actual)

    # Assert that actual data matches expected data
    assert df_actual.equals(df_expected), f"Data comparison failed to load"


# Function to check if a given file exists in the given location
def check_file_exists(file_path):
    return os.path.isfile(file_path)


# Function to check if a given file is not a zero-byte file (empty file)
def check_file_size(file_path):
    return os.path.getsize(file_path) != 0


@pytest.mark.regression
def test_extraction_from_product_data_CSV_to_product_staging_MySQL():
    logger.info("Data extraction from product_data.csv to product_staging has started .......")
    try:
        file_to_db_verify('../Testdata/product_data.csv', 'csv', 'staging_product', mysql_engine)
        logger.info("Data extraction from product_data.csv to product_staging has completed .......")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")
        pytest.fail(f"Test failed due to an error {e}")


@pytest.mark.regression
def test_extraction_from_supplier_data_JSON_to_supplier_staging_MySQL():
    logger.info("Data extraction from supplier_data.json to supplier_staging has started .......")
    try:
        file_to_db_verify('../Testdata/supplier_data.json', 'json', 'staging_supplier', mysql_engine)
        logger.info("Data extraction from supplier_data.json to supplier_staging has completed .......")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")
        pytest.fail(f"Test failed due to an error {e}")


@pytest.mark.regression
def test_extraction_from_inventory_data_XML_to_staging_inventory_MySQL():
    logger.info("Data extraction from inventory_data.xml to staging_inventory has started .......")
    try:
        file_to_db_verify('../Testdata/inventory_data.xml', 'xml', 'staging_inventory', mysql_engine)
        logger.info("Data extraction from inventory_data.xml to staging_inventory has completed .......")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")
        pytest.fail(f"Test failed due to an error {e}")
