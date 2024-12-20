from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator


# Функція для обробки результатів з XCom
def fetch_countries(**kwargs):
    # Підключення до BigQuery
    hook = BigQueryHook()
    
    # Виконання запиту
    query = """SELECT DISTINCT Country 
                FROM `starlit-wharf-442509-g1.global_health_data_bq.global_data`
                ORDER BY 1"""
    results = hook.get_pandas_df(sql=query, dialect="standard")
    
    # Перетворюємо результати на список
    countries = results["Country"].tolist()
    print(f"Отримані країни: {countries}")
    return countries


# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG definition
with DAG(
    dag_id='load_csv_to_bigquery',
    default_args=default_args,
    description='Load a CSV file from GCS to BigQuery',
    schedule_interval='@daily', 
    start_date=datetime(2024, 12, 20),
    catchup=True,
    tags=['bigquery', 'gcs', 'csv'],
) as dag:

    # Task to check if the file exists in GCS
    check_file_exists = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket='global-health-data-bucket', 
        object='global_health_data.csv', 
        timeout=300,  # Maximum wait time in seconds
        poke_interval=30,  # Time interval in seconds to check again
        mode='poke',  # Use 'poke' mode for synchronous checking
    )

    # Task to load CSV from GCS to BigQuery
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='global-health-data-bucket',  
        source_objects=['global_health_data.csv'],  
        destination_project_dataset_table='starlit-wharf-442509-g1.global_health_data_bq.global_data',  
        source_format='CSV', 
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE',  # Options: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        skip_leading_rows=1,  # Skip header row
        field_delimiter=',',  # Delimiter for CSV, default is ','
        autodetect=True,  # Automatically infer schema from the file
        #google_cloud_storage_conn_id='google_cloud_default',  
        #bigquery_conn_id='google_cloud_default',  
    )

    # Таск для виконання запиту на список країн в BigQuery 
    query_countries = PythonOperator(
        task_id="query_countries",
        python_callable=fetch_countries,
        provide_context=True,
    )

    # Define task dependencies
    check_file_exists >> load_csv_to_bigquery >> query_countries