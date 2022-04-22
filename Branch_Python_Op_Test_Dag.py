import os
import airflow
from datetime import datetime,timedelta
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator


default_args = { 'owner' = 'airflow',
    'start_date' : datetime(2022,4,12),
    'retries' : 1,
    'retry_delay' : timedelta(seconds=15)
}


dag = DAG(
    dag_id='Branch_Operator_Test_Dag',
    default_args=default_args,
    schedule_interval=None)
    

def sum_of_digits(**kwargs):
    var2=kwargs['dag_run'].conf.get("Input_Number")    #Need to pass while running the DAG {"Input_Number":4}  from UI
    #var1=kwargs['var1']
    final_sum=10+var2
    return final_sum

def task_id_to_execute(**kwargs):
    sum_value=kwargs['ti'].xcom_pull(task_ids='Add_Var1_to_10',key='return_value')
    print('Sum of Digits is:', sum_value)
    if sum_value>10:
        return 'Task_Greater_Than_20'
    return 'Task_Less_Than_20' 
    
Add_Var1_to_10 = PythonOperator( task_id='Add_Var1_to_10',
    python_callable=sum_of_digits,
    #op_kwargs={'var1':5'},
    dag=dag)
    
Fork_Task = BranchPythonOperator( task_id='Branching_Task',
    python_callable=task_id_to_execute,
    dag=dag)
    

Task_Greater_Than_20 = DummyOperator( task_id='Task_Greater_Than_20', dag=dag)

Task_Less_Than_20 = DummyOperator( task_id='Task_Less_Than_20', dag=dag)


Add_Var1_to_10 >> Fork_Task >> [Task_Greater_Than_20,Task_Less_Than_20]
    
