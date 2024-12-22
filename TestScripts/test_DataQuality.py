
# Assignment for completing the file size check for all the other file/types

import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging
import pytest
import os

# Configure the logging
from CommonUtilities.utilities import file_to_db_verify, db_to_db_verify, check_file_exists, check_file_size

import logging
import os

# Ensure Logs directory exists
if not os.path.exists('Logs'):
    os.makedirs('Logs')

# Logging mechanism
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Check if handlers exist to prevent duplicate logging
if not logger.handlers:
    handler = logging.FileHandler('Logs/etlProcess.log')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# create mysql database commection
from Config.config import *


mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

@pytest.mark.DQ_Check
def test_DQ_Sales_Data_File_Availability():
    try:
        logger.info("File availability check for Sales_data.csv has been initiated.. ")
        assert check_file_exists("../TestData/sales_data.csv"),"Sales_data file is not available"
        logger.info("File availability check for Sales_data.csv has been completed.. ")
    except Exception as e:
        logger.error(f"Error during file availability check{e}")
        pytest.fail(f"Test failed due to sales_data.csv file unavailability")

# Assignment for completing the file availability check for all the other file/types

@pytest.mark.DQ_Check
def test_DQ_Sales_Data_File_SizeCheck():
    try:
        logger.info("File size check for Sales_data.csv has been initiated.. ")
        assert check_file_size("../TestData/sales_data.csv"),"Sales_data file is zero byte file"
        logger.info("File size check for Sales_data.csv has been completed.. ")
    except Exception as e:
        logger.error(f"Error during file size check{e}")
        pytest.fail(f"Test failed due to sales_data.csv due to zero byte file")

