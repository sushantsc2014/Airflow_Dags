import airflow
from datetime import datetime,timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_to_bg import GoogleCloudStorageToBigQueryOperator

default_args = { 'owner' = 'airflow',
    'start_date' : datetime(2022,4,12),
    'retries' : 1,
    'retry_delay' : timedelta(seconds=15)
}

project_id='your_project_id_name'
stagging_dataset='dataset_of_table'
gs_bucket='bucket_name_here_without_gs//:'


dag = DAG(
    dag_id='Cloud_To_BQ_Table',
    default_args=default_args,
    schedule_interval=None)
    
    
load_bq_table = GoogleCloudStorageToBigQueryOperator(
    task_id='load_bq_table',
    bucket=gs_bucket,
    source_objects=['dags/drug-makrplace/batch/dda/*.csv'],    #path to CSV fil in bucket
    destination_project_dataset_table = f'{project_id}:{stagging_dataset}.Employee_Details',
    write_disposition='WRITE_TRUNCATE',
    source_format='csv',
    skip_leading_rows=1
    schema_object='dags/drug-makrplace/batch/dda/Table_Schema_Format.json',    #path to json file
    dag=dag
)


Dummy_Task=DummyOperator(task_id='Dummy_Task',dag=dag)

load_bq_table >> Dummy_Task