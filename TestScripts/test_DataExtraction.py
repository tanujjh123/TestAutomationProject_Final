import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle

from CommonUtilities.utilities import file_to_db_verify
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

def test_extraction_from_sales_data_CSV_to_sales_staging_MySQL():
    '''
    df_expected = pd.read_csv('Testdata/sales_data.csv')
    query = """select * from staging_sales"""
    df_actual = pd.read_sql(query, mysql_engine)
    assert df_actual.equals(df_expected),"Data extraction process failed - please check"
'''
    #file_to_db_verify('Testdata/sales_data.csv', 'csv', 'staging_sales', 'mysql_engine')
    file_to_db_verify('Testdata/sales_data.csv','csv','staging_sales',mysql_engine)