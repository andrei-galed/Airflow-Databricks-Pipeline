import requests
import json
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago


#--> traer datos de una api
# https://jsonplaceholder.typicode.com/users
#--> almacenarlo en una BD

def get_api_data(ti):

    response = requests.get("https://jsonplaceholder.typicode.com/users")
    data = response.json()
    for user in data:
        if user["id"] == 1:
            data_dict = transform(user)
            ti.xcom_push(key="data_from_api",value=data_dict)

    print(data_dict)

def transform(user):
    keys = ["id","name","username","email"]
    new_dict = {key: user[key] for key in keys if key in user}

    return new_dict

def save_to_postgres(ti):
    data = ti.xcom_pull(key="data_from_api")
    postgreshook = PostgresHook(postgres_conn_id="postgres_datapath")
    ##postgreshook sirve para subir los datos a postgres
    name = data["name"]
    username = data["username"]
    email = data["email"]
    postgreshook.run(f"INSERT INTO users (name,username,email) VALUES ('{name}','{username}','{email}');")


default_args = {
    "owner": "andrei",
    "email":["bryan@gmail.com"],
    "email_on_failure":True,
    "email_on_retry":False,
    "retries":1,
    "retry_delay":timedelta(minutes=2)
}


dag = DAG(
    dag_id="04_api_to_postgres",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["04_airflow"]
)

get_data = PythonOperator(
    task_id = "get_data_from_api",
    python_callable=get_api_data,
    dag=dag
)

save_data = PythonOperator(
    task_id = "save_data_to_postgres",
    python_callable=save_to_postgres,
    dag=dag
)


get_data >> save_data
