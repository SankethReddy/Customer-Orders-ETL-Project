from airflow import DAG
from astro import sql as aql
from datetime import datetime
import pandas as pd

## Since we will be working with files and tables, import these libraries
from astro.files import File
from astro.sql.table import Table

S3_FILE_PATH = "s3://sanketh-astrosdk" ## URI of the csv file in the AWS S3 Bucket
S3_CONN_ID = "aws_default" ## connection ID of the AWS account
SNOWFLAKE_CONN_ID = "snowflake_default" ## connection ID of the Snowflake account

## Snowflake tables
SNOWFLAKE_ORDERS = "orders_table"
SNOWFLAKE_FILTERED_ORDERS = "filtered_table"
SNOWFLAKE_JOINED = "joined_table"
SNOWFLAKE_CUSTOMERS = "customers_table"
SNOWFLAKE_REPORTING = "reporting_table"

@aql.transform
def filter_orders(input_table):
    return "SELECT * FROM {{input_table}} WHERE amount > 150"

@aql.transform
def join_orders_customers(filtered_orders_table, customers_table):
    return """
    SELECT c.customer_id, customer_name, order_id, purchase_date, amount, type
    FROM {{filtered_orders_table}} f JOIN {{customers_table}} c
    on f.customer_id = c.customer_id
    """

@aql.dataframe # transform SQL table into a Python dataframe; Will be able to manipulate dataframe in a Python environment with pandas
def transform_dataframe(df):
    purchase_dates = df['purchase_date'].values
    return purchase_dates

with DAG(
    dag_id = 'customer_orders',
    start_date = datetime(2023,1,1),
    schedule = '@daily', ## run everyday at midnight
    catchup = False
) as dag:
    
    orders_data = aql.load_file(
        input_file = File(
            path = S3_FILE_PATH + "/orders_data_header.csv", conn_id = S3_CONN_ID
        ),
        output_table = Table(conn_id = SNOWFLAKE_CONN_ID)
    )

    customers_table = Table(
        name = SNOWFLAKE_CUSTOMERS,
        conn_id = SNOWFLAKE_CONN_ID
    )

    joined_data = join_orders_customers(filter_orders(orders_data), customers_table)

    reporting_table = aql.merge(
        target_table = Table(
            name = SNOWFLAKE_REPORTING,
            conn_id = SNOWFLAKE_CONN_ID
        ),
        source_table = joined_data,
        target_conflict_columns = ["order_id"],
        columns = ["customer_id", "customer_name"],
        if_conflicts = "update"
    )

    purchase_dates = transform_dataframe(reporting_table)

    purchase_dates >> aql.cleanup() ## cleanup temporary tables that we have created when we use the tables without specifying their names