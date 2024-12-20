import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import paramiko

from CommonUtilities.utilities import file_to_db_verify, db_to_db_verify
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


@pytest.fixture()
def Sales_Data_From_Linux_Server():
    try:
        # Establish SSH client connection
        ssh_client = paramiko.SSHClient()
        # Automatically add the server's SSH key (if not already known)
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # Connect to the remote server
        ssh_client.connect(hostname, username=username, password=password)
        # Use SFTP to download the file from the remote server
        sftp = ssh_client.open_sftp()
        # Download the remote file to the local file path
        sftp.get(remote_file_path, local_file_path)
        print(f"File downloaded successfully from {remote_file_path} to {local_file_path}")
        # Close the SFTP session
        yield
        sftp.close()
        # Close the SSH client connection
        ssh_client.close()
    except Exception as e:
        print(f"An error occurred: {e}")



def test_extraction_from_sales_data_CSV_to_sales_staging_MySQL(Sales_Data_From_Linux_Server):
    logger.info(" Data extraction from sales_data.csv to sales_staging has started .......")
    try:
        file_to_db_verify('Testdata/sales_data_Linux.csv','csv','staging_sales',mysql_engine)
        logger.info(" Data extraction from sales_data.csv to sales_staging has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data extraction: {e}")
        pytest.fail(f"Test failed due to an error {e}")

@pytest.mark.skip
def test_extraction_from_product_data_CSV_to_product_staging_MySQL():
    logger.info(" Data extraction from product_data.csv to product_staging has started .......")
    try:
        file_to_db_verify('Testdata/product_data.csv','csv','staging_product',mysql_engine)
        logger.info(" Data extraction from product_data.csv to product_staging has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data extraction: {e}")
        pytest.fail(f"Test failed due to an error {e}")

@pytest.mark.regression
def test_extraction_from_supplier_data_JSON_to_supplier_staging_MySQL():
    logger.info(" Data extraction from supplier_data.json to supplier_staging has started .......")
    try:
        file_to_db_verify('Testdata/supplier_data.json','json','staging_supplier',mysql_engine)
        logger.info(" Data extraction from supplier_data.json to supplier_staging has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data extraction: {e}")
        pytest.fail(f"Test failed due to an error {e}")

@pytest.mark.regression
def test_extraction_from_inventory_data_XML_to_supplier_staging_MySQL():
    logger.info(" Data extraction from inventory_data.xml to inventory_staging has started .......")
    try:
        file_to_db_verify('Testdata/inventory_data.xml','xml','staging_inventory',mysql_engine)
        logger.info(" Data extraction from inventory_data.xml to inventory_staging has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data extraction: {e}")
        pytest.fail(f"Test failed due to an error {e}")

@pytest.mark.regression
def test_extraction_from_store_ORCL_to_store_staging_MySQL():
    logger.info(" Data comparision between from store table from Oracle to store_staging has started .......")
    try:
        query1 = """select * from stores"""
        query2 = """select * from staging_stores"""
        db_to_db_verify(query1,oracle_engine,query2,mysql_engine)
        logger.info(" Data comparision between from store table from Oracle to store_staging has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data extraction: {e}")
        pytest.fail(f"Test failed due to an error {e}")
