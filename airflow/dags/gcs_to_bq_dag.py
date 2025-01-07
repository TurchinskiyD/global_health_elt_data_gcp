from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
import json

project_id = 'starlit-wharf-442509-g1'
dataset_id = 'global_health_data_bq'
transform_dataset_id = 'transform_dataset'
reporting_dataset_id = 'reporting_dataset'
source_table = f'{project_id}.{dataset_id}.global_data'
bucket_name = 'global-health-data-bucket'
source_object = 'global_health_data.csv'


# Функція для обробки результатів з XCom
def fetch_countries(**kwargs):
    # Підключення до BigQuery
    hook = BigQueryHook()

    # Виконання запиту
    query = """SELECT Country
                 FROM (SELECT Country, 
                         SUM(`Population Affected`)
                         FROM `starlit-wharf-442509-g1.global_health_data_bq.global_data` 
                         WHERE country != 'Russia'
                         GROUP BY country
                         order by 2 DESC
                         LIMIT 10
                     ) AS top_country
                 ORDER BY 1"""

    results = hook.get_pandas_df(sql=query, dialect="standard")
    
    # Перетворюємо результати на список
    countries = results["Country"].tolist()
    print(f"Отримані країни: {countries}")

    # Зберігаємо список країн у глобальну змінну Airflow
    Variable.set("countries_list", json.dumps(countries))


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
    )

    # Таск для виконання запиту на список країн в BigQuery 
    query_countries = PythonOperator(
        task_id="query_countries",
        python_callable=fetch_countries,
        provide_context=True,
    )

    # Tasks 3: Create country-specific tables
    create_table_tasks = []  # List to store tasks
    create_view_tasks = []  # List to store view creation tasks
    # Отримання списку країн
    countries = json.loads(Variable.get("countries_list"))
    # countries = ['USA', 'India', 'Germany', 'Japan', 'France', 'Canada', 'Italy']
    for country in countries:
        # Перетворюємо назву країни у нижній регістр і заміняємо всі неприпустимі символи
        country_safe = country.lower().replace(" ", "_").replace("-", "_").replace(".", "_")

        # Task to create country-specific tables
        create_table_task = BigQueryInsertJobOperator(
            task_id=f'create_table_{country_safe.lower()}',
            configuration={
                "query": {
                    "query": f"""
                            CREATE OR REPLACE TABLE `{project_id}.{transform_dataset_id}.{country_safe.lower()}_table` AS
                            SELECT *
                            FROM `{source_table}`
                            WHERE country = '{country}'
                        """,
                    "useLegacySql": False,  # Use standard SQL syntax
                }
            },
        )

        # Task to create view for each country-specific table with selected columns and filter
        create_view_task = BigQueryInsertJobOperator(
            task_id=f'create_view_{country_safe.lower()}_table',
            configuration={
                "query": {
                    "query": f"""
                            CREATE OR REPLACE VIEW `{project_id}.{reporting_dataset_id}.{country_safe.lower()}_view` AS
                            SELECT 
                                `Year` AS `year`, 
                                `Disease Name` AS `disease_name`, 
                                `Disease Category` AS `disease_category`, 
                                `Prevalence Rate` AS `prevalence_rate`, 
                                `Incidence Rate` AS `incidence_rate`
                            FROM `{project_id}.{transform_dataset_id}.{country_safe.lower()}_table`
                            WHERE `Availability of Vaccines Treatment` = False
                        """,
                    "useLegacySql": False,
                }
            },
        )

        # Set dependencies for table creation and view creation
        create_table_task.set_upstream(load_csv_to_bigquery)
        create_view_task.set_upstream(create_table_task)

        create_table_tasks.append(create_table_task)  # Add table creation task to list
        create_view_tasks.append(create_view_task)  # Add view creation task to list

    # Dummy success task to run after all tables and views are created
    success_task = DummyOperator(
        task_id='success_task',
    )

    # Define task dependencies
    check_file_exists >> load_csv_to_bigquery >> query_countries
    for create_table_task, create_view_task in zip(create_table_tasks, create_view_tasks):
        query_countries >> create_table_task >> create_view_task >> success_task