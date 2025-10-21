from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


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

with DAG('databricks_dag',
  start_date = days_ago(2),
  schedule_interval = None,
  default_args = default_args
  ) as dag:
    
    opr_run_now = DatabricksRunNowOperator(
    task_id = 'run_now',
    databricks_conn_id = 'databricks_conn',
    job_id = 223004487989349
  )
    