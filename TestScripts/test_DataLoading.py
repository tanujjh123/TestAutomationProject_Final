# Assignment:
#  1. complete the remaining 2 load test cases

import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle

from CommonUtilities.utilities import file_to_db_verify, db_to_db_verify
from Config.config import *
import pytest
import logging

# Create mysql engine
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

import logging
import os

# Ensure Logs directory exists
if not os.path.exists('Logs'):
    os.makedirs('Logs')

if not os.path.exists('LoadingErrors'):
        os.makedirs('LoadingErrors')

# Logging mechanism
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Check if handlers exist to prevent duplicate logging
if not logger.handlers:
    handler = logging.FileHandler('Logs/TestResults.log')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

@pytest.mark.smoke
@pytest.mark.regression
def test_fact_sales_table_load():
    logger.info("test_fact_sales_table_load test has started .......")
    try:
        query_expected = """select sales_id,product_id,store_id,quantity,total_amount as total_sales ,sale_date from sales_with_deatils order by sales_id,product_id,store_id ;"""
        query_actual = """select sales_id,product_id,store_id,quantity,total_sales,sale_date from fact_sales order by sales_id,product_id,store_id ;"""
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine,'LoadingErrors/fact_Sales_differences.csv')
        logger.info("test_fact_sales_table_load test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")


@pytest.mark.smoke
@pytest.mark.regression
def test_monthly_sales_summary_table_load():
    logger.info("test_monthly_sales_summary_table_load test has started .......")
    try:
        query_expected = """select * from  monthly_sales_summary_source order by product_id,month,year;"""
        query_actual = """select * from  monthly_sales_summary order by product_id,month,year;"""
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine,'LoadingErrors/Monthly_Sales_Differances.csv')
        logger.info("test_monthly_sales_summary_table_load test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

@pytest.mark.smoke
@pytest.mark.regression
def test_inventory_fact_load():
    logger.info("test_inventory_fact_load test has started .......")
    try:
        query_expected = """select * from  staging_inventory order by product_id,store_id;"""
        query_actual = """select * from  fact_inventory order by product_id,store_id;"""
        db_to_db_verify(query_expected, mysql_engine, query_actual, mysql_engine,'LoadingErrors/inventory_Fact_differences.csv')
        logger.info("test_inventory_fact_load test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")


@pytest.mark.smoke
@pytest.mark.regression
def test__inventory_levels_by_store_fact_load():
    logger.info("test__inventory_levels_by_store_fact_load test has started .......")
    try:
        query_expected = """select * from  aggregated_inventory_levels order by store_id;"""
        query_actual = """select store_id,cast(total_inventory as Double) as total_inventory from inventory_levels_by_store;"""
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine,'LoadingErrors/Inventory_Levels_By_Store_differences.csv')
        logger.info("test__inventory_levels_by_store_fact_load test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")
