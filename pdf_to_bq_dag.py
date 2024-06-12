from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import os
from io import BytesIO
import pandas as pd
import PyPDF2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pdf_to_bq',
    default_args=default_args,
    description='A DAG to extract data from PDF and load into BigQuery',
    schedule_interval='@daily',
)

def extract_data_from_pdf():
    # Retrieve PDF file from GCP bucket
    gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id='google_cloud_default')
    pdf_content = gcs_hook.download(bucket_name='your_bucket_name', object_name='your_pdf.pdf')

    # Extract data from PDF
    pdf_reader = PyPDF2.PdfReader(BytesIO(pdf_content))
    text = ''
    for page in pdf_reader.pages:
        text += page.extract_text()
    
    # Convert extracted text into dataframe (this depends on your PDF structure)
    # Example:
    df = pd.DataFrame([text.split()], columns=['Column1', 'Column2', ...])
    
    return df

def validate_data(df):
    # Perform data validation
    # Example: check for missing values, data types, etc.
    pass

def load_data_to_bq(ds, **kwargs):
    # Insert data into BigQuery table
    bq_hook = BigQueryHook(bigquery_conn_id='bigquery_default')
    df = kwargs.get('task_instance').xcom_pull(task_ids='extract_data')
    bq_hook.insert_rows_from_dataframe('your_dataset.your_table', df)

extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_from_pdf,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    op_kwargs={'df': '{{ task_instance.xcom_pull(task_ids="extract_data") }}'},
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_to_bq',
    python_callable=load_data_to_bq,
    provide_context=True,
    dag=dag,
)

extract_data_task >> validate_data_task >> load_data_task
