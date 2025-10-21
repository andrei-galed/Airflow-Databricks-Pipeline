# Airflow-Databricks-Pipeline
Este repositorio contiene todo lo necesario para orquetar la ingesta, analisis de datos, carga y orquetacion del pipeline por medio de Airflow Y databricks 

Para ejecutar el programa debes levantar el contenedor Docker el cual contiene la imagen con todo lo   neceserio para poder levantar el servicio de Airflow. 

En la carpeta dags 
Se encuentra el archivo databricks_dag el cual tiene el dag creado para orquetar el pipeline 

En la carpeta Databricks 
Se encuentran los scripts para generar el analisis asi como enviar el correo electronico por medio del servicio de databricks. 

El archivo salaries.csv es el que se uso. 