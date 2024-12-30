import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import pytest
import logging
import os
from CommonUtilities.utilities import file_to_db_verify, db_to_db_verify
from Config.config import *

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

# Create MySQL engine
mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

@pytest.mark.smoke
@pytest.mark.regression
def test_filter_transformation():
    logger.info("Filter transformation test has started .......")
    try:
        query_expected = """select * from staging_sales where sale_date <='2024-09-20'"""
        query_actual = """select * from filtered_sales"""
        db_to_db_verify(query_expected, mysql_engine, query_actual, mysql_engine,'DBErrors/Filter_Transformation_Error.csv')
        logger.info("Filter transformation test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

@pytest.mark.regression
def test_Router_High_region_sales_transformation():
    logger.info("test_Router_High_region_sales_transformation test has started .......")
    try:
        query_expected = """select * from filtered_sales where region='High'"""
        query_actual = """select * from high_sales"""
        db_to_db_verify(query_expected, mysql_engine, query_actual, mysql_engine,'DBErrors/Router_data_Transformation_differences.csv')
        logger.info("test_Router_High_region_sales_transformation test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

@pytest.mark.smoke
def test_Router_Low_region_sales_transformation():
    logger.info("test_Router_Low_region_sales_transformation test has started .......")
    try:
        query_expected = """select * from filtered_sales where region='Low'"""
        query_actual = """select * from low_sales"""
        db_to_db_verify(query_expected, mysql_engine, query_actual, mysql_engine,'DBErrors/Low_Region_Sales_Transformation_differences.csv')
        logger.info("test_Router_Low_region_sales_transformation test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

def test_Aggregate_Sales_data_transformation():
    logger.info("test_Aggregate_Sales_data_transformation test has started .......")
    try:
        query_expected = """select product_id, month(sale_date) as month, year(sale_date) as year, sum(quantity * price) as total_sales from filtered_sales group by product_id, month(sale_date), year(sale_date)"""
        query_actual = """select * from monthly_sales_summary_source"""
        db_to_db_verify(query_expected, mysql_engine, query_actual, mysql_engine,'DBErrors/Aggregate_Sales_Data_differences.csv')
        logger.info("test_Aggregate_Sales_data_transformation test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

def test_JOINER_transformation():
    logger.info("test_JOINER_transformation test has started .......")
    try:
        query_expected = """select s.sales_id, s.product_id, s.store_id, p.product_name, st.store_name, s.quantity, s.price * s.quantity as total_amount, s.sale_date from filtered_sales as s join staging_product as p on s.product_id = p.product_id join staging_stores as st on s.store_id = st.store_id"""
        query_actual = """select * from sales_with_deatils"""
        db_to_db_verify(query_expected, mysql_engine, query_actual, mysql_engine,'DBErrors/Joiner_Transformation_differences.csv')
        logger.info("test_JOINER_transformation test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

def test_aggregate_inventory_levels_transformation():
    logger.info("test_aggregate_inventory_levels_transformation test has started .......")
    try:
        query_expected = """select store_id, sum(quantity_on_hand) as total_inventory from staging_inventory group by store_id"""
        query_actual = """select * from aggregated_inventory_levels"""
        db_to_db_verify(query_expected, mysql_engine, query_actual, mysql_engine,'DBErrors/Aggregare_Levels_Transformation_differences.csv')
        logger.info("test_aggregate_inventory_levels_transformation test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")
