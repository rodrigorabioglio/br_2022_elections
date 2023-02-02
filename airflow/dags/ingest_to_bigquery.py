from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from operators import (
    download_files_and_unzip,
    upload_to_gcs_bucket,
    delete_files,
)

from const import DATA_DIR, BUCKET_NAME

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="@daily",
    # start_date=datetime(2022, 10, 31),
    # end_date=datetime(2022, 11, 27),
    start_date=datetime(2022, 11, 1),
    end_date=datetime(2022, 11, 2),
    max_active_tasks=1,
    max_active_runs=1
)

with local_workflow:

    download_files_task = PythonOperator(
        task_id="download_files",
        python_callable=download_files_and_unzip,
        provide_context=True,
        op_kwargs=dict(
            path=DATA_DIR,
        )
    )

    upload_to_gcs_bucket_task = PythonOperator(
        task_id="upload_to_bucket",
        python_callable=upload_to_gcs_bucket,
        provide_context=True,
        op_kwargs=dict(
            local_data_dir=DATA_DIR,
            bucket_name=BUCKET_NAME,
        )
    )

    ingest_to_bigquery_task = BashOperator(
        task_id="ingest_to_bigquery",
        # bash_command='ls -la'
        bash_command='gcloud dataproc jobs submit pyspark --cluster=sparkito --region=us-central1 /opt/airflow/dags/spark_job.py\
            --jars="gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.28.0.jar" \
            --py-files=/opt/airflow/deps/deps.zip \
             -- -b=de-boootcamp-root -ds="{{ ds }}"'
    )

    delete_files_task = PythonOperator(
        task_id='delete_files',
        python_callable=delete_files,
        provide_context=True,
        op_kwargs=dict(
            path=DATA_DIR,
        )
    )

    download_files_task >> upload_to_gcs_bucket_task >> [delete_files_task, ingest_to_bigquery_task]