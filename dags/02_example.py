from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

# calcular el día actual
# fin de semana --> PythonOperator  ---> sinifica que vamos a usar funciones
# día de semana --> BashOperator

def branch_function():
    if datetime.now().weekday() >= 5:
        return 'python_task' 
    else:
        return 'bash_task'

def python_function():
    print("¡Es fin de semana!")

default_args = {
    'owner': 'andrei',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

with DAG(
    '02_example',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval='@daily',
    tags=['02_airflow'],
) as dag:

    start = EmptyOperator(task_id='start')

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_function,
    )

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=python_function,
    )

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "¡Es día de semana!"',
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule='all_done',
        #all_done sirve para que al final se ejecute o se salte la tarea final 
    )

    start >> branch_task >> [python_task, bash_task] >> end
