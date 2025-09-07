import os
import json
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from google.cloud import storage
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from vgsi.vgsi_utils import load_city

# set default arguments
afw_default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 25),
    "depends_on_past": False,
    "retries": 1,
    'retry_delay': timedelta(minutes=5),
    'city': 'new_haven'
}

# environmental variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")
BQ_DATASET_NAME = os.environ.get("BQ_DATASET_NAME", 'stg_properties_dataset')
PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PREFIX_DATASET_FILE = "{{ data_interval_end.strftime(\'%m%d_%H%M\') }}"
DATA_CATEGORIES = ["property", "building", "assesment", "appraisal", "ownership"]

RUN_DATE = datetime.today().strftime('%Y-%m-%d')
# PARQUET_FILENAME = DATASET_FILE.replace('.json', '.parquet')

def download_city(city, output_parquet_file):
    property_df, building_df, assesment_df, appraisal_df, ownership_df = load_city(city, delay_seconds=0)

    property_df.to_parquet(output_parquet_file + f"_{city}_property.parquet", index=False)
    building_df.to_parquet(output_parquet_file + f"_{city}_building.parquet", index=False)
    assesment_df.to_parquet(output_parquet_file + f"_{city}_assesment.parquet", index=False)
    appraisal_df.to_parquet(output_parquet_file + f"_{city}_appraisal.parquet", index=False)
    ownership_df.to_parquet(output_parquet_file + f"_{city}_ownership.parquet", index=False)

def upload_to_gcs(bucket_name, city, output_parquet_file, data_categories, run_date):
    
    # create a client for gcs
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for category in data_categories:

        path = f"{output_parquet_file}_{city}_{category}.parquet"

        # upload data
        object_name = f"raw/parquet/{category}/{run_date}_{city}.parquet"
        blob = bucket.blob(object_name)
        blob.upload_from_filename(path, timeout=300)

afw_default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 4),
    "depends_on_past": False,
    "retries": 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="scrape_vgsi",
    schedule='@weekly',
    default_args= afw_default_args,
    max_active_runs=1,
    catchup = False,
    tags=['ct-properties']
) as dag:

    download_data_task = PythonOperator(
        task_id = "download_data",
        python_callable = download_city,
        op_kwargs={
            "city": "new_haven",
            "output_parquet_file": f"{PATH_TO_LOCAL_HOME}/{PREFIX_DATASET_FILE}"
        }
        
    )

     # upload the raw data to gcs
    local_to_gcs_task = PythonOperator(
        task_id="upload_data_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "output_parquet_file": f"{PATH_TO_LOCAL_HOME}/{PREFIX_DATASET_FILE}",
            "city": "new_haven",
            "data_categories": DATA_CATEGORIES,
            "run_date": RUN_DATE
        }
    )

    load_property_bq = GCSToBigQueryOperator(
        task_id="load_property_bq",
        bucket=BUCKET_NAME,
        source_objects = [f"raw/parquet/property/{RUN_DATE}_new_haven.parquet"],
        source_format='PARQUET',
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET_NAME}.raw_properties',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )

    load_building_bq = GCSToBigQueryOperator(
        task_id="load_building_bq",
        bucket=BUCKET_NAME,
        source_objects = [f"raw/parquet/building/{RUN_DATE}_new_haven.parquet"],
        source_format='PARQUET',
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET_NAME}.raw_buildings',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )

    load_assesments_bq = GCSToBigQueryOperator(
        task_id="load_assesments_bq",
        bucket=BUCKET_NAME,
        source_objects = [f"raw/parquet/assesments/{RUN_DATE}_new_haven.parquet"],
        source_format='PARQUET',
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET_NAME}.raw_assesments',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )

    load_appraisals_bq = GCSToBigQueryOperator(
        task_id="load_appraisals_bq",
        bucket=BUCKET_NAME,
        source_objects = [f"raw/parquet/appraisals/{RUN_DATE}_new_haven.parquet"],
        source_format='PARQUET',
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET_NAME}.raw_appraisals',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )

    load_ownership_bq = GCSToBigQueryOperator(
        task_id="load_appraisals_bq",
        bucket=BUCKET_NAME,
        source_objects = [f"raw/parquet/ownership/{RUN_DATE}_new_haven.parquet"],
        source_format='PARQUET',
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET_NAME}.raw_ownership',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )

    download_data_task >> local_to_gcs_task >> local_to_gcs_task >> [load_building_bq, load_assesments_bq, load_appraisals_bq, load_ownership_bq]