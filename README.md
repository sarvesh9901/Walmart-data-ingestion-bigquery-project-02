# Walmart Data Injection using Airflow and BigQuery

## Overview
This project automates the process of loading Walmart sales and merchant data from a Google Cloud Storage (GCS) bucket into BigQuery using Apache Airflow. The workflow involves creating datasets and tables, loading data from GCS to BigQuery, and merging transformed data into the target table.

## Tech Stack
- **Google Cloud Platform (GCP)**
  - Cloud Storage (GCS) - Data Storage
  - BigQuery - Data Warehouse
  - Cloud Composer (Managed Apache Airflow) - Workflow Orchestration
- **Apache Airflow**
  - DAG (Directed Acyclic Graph) - Task Scheduling
  - `BigQueryCreateEmptyDatasetOperator` - Dataset Creation
  - `BigQueryCreateEmptyTableOperator` - Table Creation
  - `GCSToBigQueryOperator` - Data Load
  - `BigQueryExecuteQueryOperator` - Query Execution (Merging Data)
- **Python** - Airflow DAG Development

## Project Workflow

### 1. Define Default DAG Arguments
A dictionary `default_dag_args` is defined to set default arguments such as start date, retries, and retry delay.

### 2. Initialize DAG Object
A DAG (`walmart_data_pipeline`) is created to schedule and automate the workflow.

### 3. Create BigQuery Dataset
The dataset is created in BigQuery using `BigQueryCreateEmptyDatasetOperator()`.

### 4. Create BigQuery Tables
Three tables are created in BigQuery using `BigQueryCreateEmptyTableOperator()`:
- `merchant_tb`
- `sales_stage`
- `sales_target`

Each task is configured with parameters such as `task_id`, `dataset_id`, `table_id`, and schema definitions.

### 5. Load Data into `merchant_tb` and `sales_stage`
The `GCSToBigQueryOperator()` is used to load data from GCS into BigQuery tables (`merchant_tb` and `sales_stage`). A **TaskGroup** in Airflow is used to manage these data load operations efficiently.

### 6. Merge Data into `sales_target`
The `BigQueryExecuteQueryOperator()` is used to merge data from `sales_stage` and `merchant_tb` into `sales_target`. This step executes a SQL query that joins the two tables and inserts the results into `sales_target`.

### 7. Define Task Flow
The task dependencies are set in the following sequence:
```plaintext
create_dataset >> [create_merchants_table, create_walmart_sales_table, create_target_table] >> load_data
load_data >> merge_walmart_sales
```

This ensures that:
1. The dataset is created first.
2. The tables are created next.
3. Data is loaded into the respective tables.
4. The final merge operation takes place after data is loaded.

## Conclusion
This project successfully automates the Walmart data ingestion pipeline using Airflow and BigQuery. The workflow ensures seamless data loading and transformation, enabling efficient data analysis and reporting.


