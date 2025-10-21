from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.models.param import Param

# Definir 3 funciones de los cuales, una va a recibir el nombre de la persona y otra va a recibir el apellido de la persona, por params
# la segunda va a recibir la edad de la persona, por params
# Ambas funciones van a subir estos datos por XCOM
# la tercera función va a recibir los datos de las 2 funciones anteriores y va a imprimir el nombre completo y edad de la persona


def get_name(ti,**context):
    dag_params = context["params"]
    nombre_param = dag_params.get("nombre")
    apellido_param = dag_params.get("apellido")

    #sirve para subir los valores de "nombre" y "apellido" por xcom_push
    ti.xcom_push(key="nombre",value=nombre_param)
    ti.xcom_push(key="apellido",value=apellido_param)

def get_age(ti,**context):
    dag_params = context["params"]
    edad = dag_params.get("edad")

    ti.xcom_push(key="edad",value=edad)

#funcion para extraer lo valores obtenidos
#xcom_pull es pra jalar los datos de xcom
def extract(ti):

    nombre = ti.xcom_pull(task_ids="get_name",key="nombre")
    apellido = ti.xcom_pull(task_ids="get_name",key="apellido")
    edad = ti.xcom_pull(task_ids="get_age",key="edad")

    print("-----------------------")
    print(nombre+" - "+apellido+" y tiene "+str(edad)+" años")
    print("-----------------------")


default_args = {
    'owner': 'bryan',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

#El valor de None permite ejecutar el DAG de manera manual 
with DAG(
    '03_example',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval=None, #'@daily',
    tags=['03_airflow'],
    params={
        "nombre": Param("",type="string"),
        "apellido": Param("",type="string"),
        "edad": Param(10,type="integer",minimum=10,maximum=100)
    }
) as dag:

    taskA = PythonOperator(
        task_id="get_name",
        python_callable=get_name,
        provide_context=True
        ##provide:context permite que se puede utilizar 
        ## **context usados en las funciones
    )

    taskB = PythonOperator(
        task_id="get_age",
        python_callable=get_age,
        provide_context=True
    )

    taskC = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    [taskA,taskB] >> taskC
