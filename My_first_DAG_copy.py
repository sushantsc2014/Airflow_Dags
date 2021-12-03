import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
#added comment

args = {
    'owner': 'airflow',
    'reties': '2',
    'retry_delay': timedelta(seconds=10)
}

dag= DAG(
    dag_id='My_first_DAG',
    catchup=False,
    default_args=args,
    max_active_runs=1,
    schedule_interval=None,
)


Wnd = BashOperator(task_id="print_date",
                    bash_command='echo "Hello Airflo"',
                    dag=dag) 
