import os.path
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import paramiko
import pytest
import logging

from CommonUtilities.utilities import file_to_db_verify, db_to_db_verify
from Config.config import *

# Create MySQL engine
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

# Create Oracle engine
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/?service_name={ORACLE_SERVICE}&mode=SYSDBA')

# Ensure Logs directory exists
if not os.path.exists('Logs'):
    os.makedirs('Logs')

# Logging mechanism
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Check if handlers exist to prevent duplicate logging
if not logger.handlers:
    handler = logging.FileHandler('Logs/TestResults.log')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

@pytest.mark.regression
def test_extraction_from_product_data_CSV_to_product_staging_MySQL():
    logger.info("Data extraction from product_data.csv to product_staging has started .......")
    try:
        file_to_db_verify('../Testdata/product_data.csv', 'csv', 'staging_product', mysql_engine, 'Logs/product_data_differences.csv')
        logger.info("Data extraction from product_data.csv to product_staging has completed .......")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")
        pytest.fail(f"Test failed due to an error {e}")

@pytest.mark.regression
def test_extraction_from_supplier_data_JSON_to_supplier_staging_MySQL():
    logger.info("Data extraction from supplier_data.json to supplier_staging has started .......")
    try:
        file_to_db_verify('../Testdata/supplier_data.json', 'json', 'staging_supplier', mysql_engine, 'Logs/supplier_data_differences.csv')
        logger.info("Data extraction from supplier_data.json to supplier_staging has completed .......")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")
        pytest.fail(f"Test failed due to an error {e}")

@pytest.mark.regression
def test_extraction_from_inventory_data_XML_to_staging_inventory_MySQL():
    logger.info("Data extraction from inventory_data.xml to inventory_staging has started .......")
    try:
        file_to_db_verify('../Testdata/inventory_data.xml', 'xml', 'staging_inventory', mysql_engine, 'Logs/inventory_data_differences.csv')
        logger.info("Data extraction from inventory_data.xml to inventory_staging has completed .......")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")
        pytest.fail(f"Test failed due to an error {e}")

@pytest.mark.regression
def test_extraction_from_store_ORCL_to_store_staging_MySQL():
    logger.info("Data comparison between store table from Oracle to store_staging has started .......")
    try:
        query1 = """SELECT * FROM stores"""
        query2 = """SELECT * FROM staging_stores"""
        db_to_db_verify(query1, oracle_engine, query2, mysql_engine, 'Logs/store_data_differences.csv')
        logger.info("Data comparison between store table from Oracle to store_staging has completed .......")
    except Exception as e:
        logger.error(f"Error occurred during data extraction: {e}")
        pytest.fail(f"Test failed due to an error {e}")
