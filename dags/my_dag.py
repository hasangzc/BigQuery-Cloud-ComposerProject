from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator



GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID = "your_project_id"
GS_PATH = "airbnb/"
BUCKET_NAME = "airbnbb"
STAGING_DATASET = "airbnb_staging_dataset"
DATASET = "airbnb_dataset"
LOCATION = "us-central1"


default_args = {
    'owner': 'Your Name',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

with DAG('AirbnbWarehouse', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
    )

    load_staging_dataset = DummyOperator(
        task_id = 'load_staging_dataset',
        dag = dag
    )


     # Google Cloud Storage to BigQuery
    load_dataset_review = GCSToBigQueryOperator(
        task_id = 'load_dataset_review',
        bucket = BUCKET_NAME,
        source_objects = ['airbnb/airbnb_last_review.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_review',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'listing_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'host_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'last_review', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )



    load_dataset_price = GCSToBigQueryOperator(
        task_id = 'load_dataset_price',
        bucket = BUCKET_NAME,
        source_objects = ['airbnb/airbnb_price.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_price',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'listing_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'price', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'nbhood_full', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )


    load_dataset_room = GCSToBigQueryOperator(
        task_id = 'load_dataset_room',
        bucket = BUCKET_NAME,
        source_objects = ['airbnb/airbnb_room_type.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_room',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'listing_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'room_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )


    check_dataset_review = BigQueryCheckOperator(
        task_id = 'check_dataset_review',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_review`'
        )


    check_dataset_price = BigQueryCheckOperator(
        task_id = 'check_dataset_price',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_price`'
        )

    check_dataset_room = BigQueryCheckOperator(
        task_id = 'check_dataset_room',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_room`'
        ) 

    create_Result_Table = DummyOperator(
        task_id = 'Create_Result_Table',
        dag = dag
        )

    create_dataset_airbnb = BigQueryOperator(
        task_id = 'create_dataset_airbnb',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/query.sql'
        )


    check_dataset_airbnb = BigQueryCheckOperator(
        task_id = 'check_dataset_airbnb',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.airbnb_data`'
        ) 

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        ) 

start_pipeline >> load_staging_dataset

load_staging_dataset >> [load_dataset_review, load_dataset_price, load_dataset_room]

load_dataset_review >> check_dataset_review
load_dataset_price >> check_dataset_price
load_dataset_room >> check_dataset_room

[check_dataset_review, check_dataset_price, check_dataset_room] >> create_Result_Table

create_Result_Table >> create_dataset_airbnb >> check_dataset_airbnb >> finish_pipeline

