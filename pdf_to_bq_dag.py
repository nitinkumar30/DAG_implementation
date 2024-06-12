from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
import os
from io import BytesIO
import pandas as pd
import PyPDF2

# Define default arguments for the DAG
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG with the specified default arguments and schedule interval
DAG_ID = 'PDF_TO_BQ'
dag = DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description='A DAG to extract data from PDF and load into BigQuery',
    schedule_interval='@daily',
)

# Function to extract data from the PDF file
def extract_data_from_pdf():
    """
    Extracts data from a PDF file stored in Google Cloud Storage.
    """
    # Retrieve PDF file from GCP bucket
    GCS_HOOK = GoogleCloudStorageHook(google_cloud_storage_conn_id='GOOGLE_CLOUD_DEFAULT')
    PDF_CONTENT = GCS_HOOK.download(bucket_name='YOUR_BUCKET_NAME', object_name='YOUR_PDF.pdf')

    # Extract data from PDF
    PDF_READER = PyPDF2.PdfReader(BytesIO(PDF_CONTENT))
    TEXT = ''
    for page in PDF_READER.pages:
        TEXT += page.extract_text()
    
    # Convert extracted text into dataframe (this depends on your PDF structure)
    # Example:
    DF = pd.DataFrame([TEXT.split()], columns=['Column1', 'Column2', ...])
    
    return DF

# Function to validate the extracted data
def validate_data(DF):
    """
    Validates the extracted data before loading it into BigQuery.
    """
    # Perform data validation
    # Example: check for missing values, data types, etc.
    pass

# Function to load validated data into BigQuery
def load_data_to_bq(ds, **kwargs):
    """
    Loads validated data into BigQuery.
    """
    # Insert data into BigQuery table
    BQ_HOOK = BigQueryHook(bigquery_conn_id='BIGQUERY_DEFAULT')
    DF = kwargs.get('task_instance').xcom_pull(task_ids='EXTRACT_DATA')
    BQ_HOOK.insert_rows_from_dataframe('YOUR_DATASET.YOUR_TABLE', DF)

# Define tasks for the DAG

# Task to extract data from PDF
EXTRACT_DATA_TASK = PythonOperator(
    task_id='EXTRACT_DATA',
    python_callable=extract_data_from_pdf,
    dag=dag,
)

# Task to validate the extracted data
VALIDATE_DATA_TASK = PythonOperator(
    task_id='VALIDATE_DATA',
    python_callable=validate_data,
    op_kwargs={'DF': '{{ task_instance.xcom_pull(task_ids="EXTRACT_DATA") }}'},
    dag=dag,
)

# Task to load validated data into BigQuery
LOAD_DATA_TASK = PythonOperator(
    task_id='LOAD_DATA_TO_BQ',
    python_callable=load_data_to_bq,
    provide_context=True,
    dag=dag,
)

# Define dependencies between tasks
EXTRACT_DATA_TASK >> VALIDATE_DATA_TASK >> LOAD_DATA_TASK
