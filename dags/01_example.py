##Importamos las librerias necesarias para Airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime


#retry_email es para notificar que habra un retry
default_args = {
    'owner': 'Andrei',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(2),
}

with DAG(
    '01_example',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval='@daily',
    tags=['01_airflow'],
) as dag:
    #empty operator son tareas vacias, es decir hacen nada 
    start = EmptyOperator(task_id='start')
    task_1 = DummyOperator(task_id='task_1')
    task_2 = DummyOperator(task_id='task_2')
    task_3 = DummyOperator(task_id='task_3')
    end = EmptyOperator(task_id='end')
#Secuencia para orquestarlo 
    start >> task_1 >> task_2 >> task_3 >> end
