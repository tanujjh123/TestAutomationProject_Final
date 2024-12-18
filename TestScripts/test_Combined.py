import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle

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

@pytest.mark.smoke
@pytest.mark.regression
@pytest.mark.sanity
@pytest.mark.xfail
def test_extraction_from_sales_data_CSV_to_sales_staging_MySQL():
    logger.info(" Data extraction from sales_data.csv to sales_staging has started .......")
    try:
        file_to_db_verify('Testdata/sales_data.csv1','csv','staging_sales',mysql_engine)
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


@pytest.mark.smoke
@pytest.mark.regression
def test_filter_transformation():
    logger.info("Filter transformation test has started .......")
    try:
        query_expected = """select * from staging_sales where sale_date <='2024-09-20'"""
        query_actual = """select * from filtered_sales"""
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)
        logger.info("Filter transformation test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

@pytest.mark.regression
def test_Rounter_High_region_sales_transformation():
    logger.info("test_Rounter_High_region_sales_transformation test has started .......")
    try:
        query_expected = """select * from filtered_sales where region='High'"""
        query_actual = """select * from high_sales"""
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)
        logger.info("test_Rounter_High_region_sales_transformation test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

@pytest.mark.smoke
def test_Rounter_Low_region_sales_transformation():
    logger.info("test_Rounter_Low_region_sales_transformation test has started .......")
    try:
        query_expected = """select * from filtered_sales where region='Low'"""
        query_actual = """select * from low_sales"""
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)
        logger.info("test_Rounter_Low_region_sales_transformation test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

def test_Aggregate_Sales_data_transformation():
    logger.info("test_Aggregate_Sales_data_transformation test has started .......")
    try:
        query_expected = """select product_id,month(sale_date) as month,year(sale_date) as year ,sum(quantity*price) as total_sales from filtered_sales
                         group by product_id,month(sale_date),year(sale_date)"""
        query_actual = """select * from monthly_sales_summary_source"""
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)
        logger.info("test_Aggregate_Sales_data_transformation test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

def test_JOINER_transformation():
    logger.info("test_JOINER_transformation test has started .......")
    try:
        query_expected = """select s.sales_id,s.product_id,s.store_id,p.product_name,st.store_name,s.quantity,
                            s.price*s.quantity as total_amount,s.sale_date
                            from filtered_sales as s
                            join staging_product as p on s.product_id = p.product_id
                            join staging_stores as st on s.store_id = st.store_id;"""
        query_actual = """select * from sales_with_deatils"""
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)
        logger.info("test_JOINER_transformation test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

def test_aggregate_inventory_levels_transformation():
    logger.info("test_aggregate_inventory_levels_transformation test has started .......")
    try:
        query_expected = """select store_id,sum(quantity_on_hand) as total_inventory from staging_inventory group by store_id;"""
        query_actual = """select * from aggregated_inventory_levels"""
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)
        logger.info("test_aggregate_inventory_levels_transformation test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")

@pytest.mark.smoke
@pytest.mark.regression
def test_fact_sales_table_load():
    logger.info("test_fact_sales_table_load test has started .......")
    try:
        query_expected = """select sales_id,product_id,store_id,quantity,total_amount as total_sales ,sale_date from sales_with_deatils order by sales_id,product_id,store_id ;"""
        query_actual = """select sales_id,product_id,store_id,quantity,total_sales,sale_date from fact_sales order by sales_id,product_id,store_id ;"""
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)
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
        db_to_db_verify(query_expected,mysql_engine,query_actual,mysql_engine)
        logger.info("test_monthly_sales_summary_table_load test has completed .......")
    except Exception as e:
        logger.error(f"Error occured during data transformation: {e}")
        pytest.fail(f"Test failed due to an error {e}")
