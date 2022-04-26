import airflow
from datetime import datetime,timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


default_args = { 'owner' = 'airflow',
    'start_date' : datetime(2022,4,12),
    'retries' : 1,
    'retry_delay' : timedelta(seconds=15)
}

dag = DAG(
    dag_id='kwargs_testing',
    default_args=default_args,
    schedule_interval=None)

def kwarg_func(**kwargs):
    print(kwargs)
    var3=kwargs['var1']
    var4=kwargs['var2']
    sum_total=var4+var3
    print(sum_total)
    
def arg_func(*args):
    print(args)
    print(args[0])
    print(args[1])


args_task_1= PythonOperator( task_id='args_task_1',
    python_callable=arg_func,
    op_args=[5,10,20],
    dag=dag)


kwargs_task_2= PythonOperator( task_id='kwargs_task_2',
    python_callable=kwarg_func,
    op_kwargs={'var1':5,'var2':10},
    dag=dag)

args_task_1 >> kwargs_task_2
