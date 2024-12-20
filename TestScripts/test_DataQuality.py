
# Assignment for completing the file size check for all the other file/types

import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging
import pytest
import os

# Configure the logging
from CommonUtilities.utilities import file_to_db_verify, db_to_db_verify, check_file_exists, check_file_size

logging.basicConfig(
    filename='Logs/etlprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)

# create mysql database commection
from Config.config import *

#mysql_engine = create_engine('mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh')
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

@pytest.mark.DQ_Check
def test_DQ_Sales_Data_File_Availability():
    try:
        logger.info("File availability check for Sales_data.csv has been initiated.. ")
        assert check_file_exists("TestData/sales_data.csv"),"Sales_data file is not available"
        logger.info("File availability check for Sales_data.csv has been completed.. ")
    except Exception as e:
        logger.error(f"Error during file availability check{e}")
        pytest.fail(f"Test failed due to sales_data.csv file unavailability")

# Assignment for completing the file availability check for all the other file/types

@pytest.mark.DQ_Check
def test_DQ_Sales_Data_File_SizeCheck():
    try:
        logger.info("File size check for Sales_data.csv has been initiated.. ")
        assert check_file_size("TestData/sales_data.csv"),"Sales_data file is zero byte file"
        logger.info("File size check for Sales_data.csv has been completed.. ")
    except Exception as e:
        logger.error(f"Error during file size check{e}")
        pytest.fail(f"Test failed due to sales_data.csv due to zero byte file")

