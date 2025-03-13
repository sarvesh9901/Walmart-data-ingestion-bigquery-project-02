from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'walmart_sales_etl_gcs',
    default_args=default_args,
    description='ETL pipeline for Walmart sales data from GCS',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Create BigQuery Dataset if it doesn't exist
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id='walmart_dwh',
        location='US'
    )

    # Task 2: Create BigQuery Tables
    create_merchants_table = BigQueryCreateEmptyTableOperator(
        task_id='create_merchants_table',
        dataset_id='walmart_dwh',
        table_id='merchants_tb',
        schema_fields=[
            {"name": "merchant_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "merchant_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "merchant_category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "merchant_country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ]
    )

    create_walmart_sales_table = BigQueryCreateEmptyTableOperator(
        task_id='create_walmart_sales_table',
        dataset_id='walmart_dwh',
        table_id='walmart_sales_stage',
        schema_fields=[
            {"name": "sale_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "sale_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "quantity_sold", "type": "INT64", "mode": "NULLABLE"},
            {"name": "total_sale_amount", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "merchant_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ]
    )

    create_target_table = BigQueryCreateEmptyTableOperator(
        task_id='create_target_table',
        dataset_id='walmart_dwh',
        table_id='walmart_sales_tgt',
        schema_fields=[
            {"name": "sale_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "sale_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "quantity_sold", "type": "INT64", "mode": "NULLABLE"},
            {"name": "total_sale_amount", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "merchant_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "merchant_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "merchant_category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "merchant_country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ]
    )

    # Task 3: Load data from GCS to BigQuery
    with TaskGroup("load_data") as load_data:

        gcs_to_bq_merchants = GCSToBigQueryOperator(
            task_id='gcs_to_bq_merchants',
            bucket='my_bigquery_projects',
            source_objects=['walmart_ingestion/merchants/merchants.json'],
            destination_project_dataset_table='turnkey-channel-358109.walmart_dwh.merchants_tb',
            write_disposition='WRITE_TRUNCATE',
            source_format='NEWLINE_DELIMITED_JSON',
        )

        gcs_to_bq_walmart_sales = GCSToBigQueryOperator(
            task_id='gcs_to_bq_walmart_sales',
            bucket='my_bigquery_projects',
            source_objects=['walmart_ingestion/sales/walmart_sales.json'],
            destination_project_dataset_table='turnkey-channel-358109.walmart_dwh.walmart_sales_stage',
            write_disposition='WRITE_TRUNCATE',
            source_format='NEWLINE_DELIMITED_JSON',
        )

    # Task 4: Perform UPSERT using MERGE Query
    merge_walmart_sales = BigQueryExecuteQueryOperator(
        task_id='merge_walmart_sales',
        sql="""
            MERGE `turnkey-channel-358109.walmart_dwh.walmart_sales_tgt` T
            USING (
              SELECT 
                S.sale_id, 
                S.sale_date, 
                S.product_id, 
                S.quantity_sold, 
                S.total_sale_amount, 
                S.merchant_id, 
                M.merchant_name, 
                M.merchant_category, 
                M.merchant_country,
                CURRENT_TIMESTAMP() as last_update
              FROM `turnkey-channel-358109.walmart_dwh.walmart_sales_stage` S
              LEFT JOIN `turnkey-channel-358109.walmart_dwh.merchants_tb` M
              ON S.merchant_id = M.merchant_id
            ) S
            ON T.sale_id = S.sale_id
            WHEN MATCHED THEN 
              UPDATE SET
                T.sale_date = S.sale_date,
                T.product_id = S.product_id,
                T.quantity_sold = S.quantity_sold,
                T.total_sale_amount = S.total_sale_amount,
                T.merchant_id = S.merchant_id,
                T.merchant_name = S.merchant_name,
                T.merchant_category = S.merchant_category,
                T.merchant_country = S.merchant_country,
                T.last_update = S.last_update
            WHEN NOT MATCHED THEN 
              INSERT (
                sale_id, sale_date, product_id, quantity_sold, total_sale_amount, 
                merchant_id, merchant_name, merchant_category, merchant_country, last_update
              )
              VALUES (
                S.sale_id, S.sale_date, S.product_id, S.quantity_sold, S.total_sale_amount, 
                S.merchant_id, S.merchant_name, S.merchant_category, S.merchant_country, S.last_update
              );
        """,
        use_legacy_sql=False
    )

    # Define Task Dependencies
    create_dataset >> [create_merchants_table, create_walmart_sales_table, create_target_table] >> load_data
    load_data >> merge_walmart_sales
