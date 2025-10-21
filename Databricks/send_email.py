# Databricks notebook source
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib
import base64
import tempfile 
import os

# COMMAND ----------

df = (spark.read.option("header",True).csv("s3://galedbucket/Data/databricks/salarios_top_10.csv"))
df_email = df.toPandas()

# Obtiene el directorio temporal del sistema.
tmpdir = tempfile.gettempdir()


#Creaamos el archivo csv
df_email.to_csv(f"{tmpdir}/salarios_top_10.csv")

#Guardamos el archivo en el directorio temporal
archivo_adjunto = f"{tmpdir}/salarios_top_10.csv"



# COMMAND ----------


# --- Configuración ---
today = datetime.today().strftime('%d-%m-%Y')
today = datetime.now(timezone.utc).strftime('%d-%m-%Y')
remitente = "datapathfunctions@gmail.com"
destinatario = "andreigaled3@gmail.com"
asunto = "Reporte CSV"
cuerpo = "Cordial saludo, \r\n\r\n\r\n" \
              f"Adjunto a este correo el reporte diario de el top 10 de salarios mas altos por tipo de trabajo " \
              f"{today}. \r\n\r\n\r\n"\
              "Quedo atento a cualquier inquietud.\r\n\r\n\r\n\r\n"\
              "Un saludo,"

contrasena = "pgazwxgdbmrtssxj"  # Para Gmail, genera una contraseña de aplicación

# --- Crear el objeto del correo ---
msg = MIMEMultipart()
msg['From'] = remitente
msg['To'] = destinatario
msg['Subject'] = asunto

# Adjuntar el cuerpo del texto
msg.attach(MIMEText(cuerpo, 'plain'))

# --- Adjuntar el archivo ---
con_archivo = open(archivo_adjunto, "rb")
parte_adjunto = MIMEBase('application', 'octet-stream')
parte_adjunto.set_payload((con_archivo).read())
encoders.encode_base64(parte_adjunto)
parte_adjunto.add_header('Content-Disposition', f"attachment; filename= {archivo_adjunto.split('/')[-1]}")
msg.attach(parte_adjunto)
con_archivo.close()


# --- Enviar el correo ---
try:
    servidor = smtplib.SMTP('smtp.gmail.com', 587)
    servidor.starttls() # Iniciar TLS para seguridad
    servidor.login(remitente, contrasena)
    texto = msg.as_string()
    servidor.sendmail(remitente, destinatario, texto)
    servidor.quit()
    print("Correo enviado exitosamente.")
except Exception as e:
    print(f"Error al enviar el correo: {e}")



# Elimina el archivo Excel del directorio temporal.
os.remove(f"{tmpdir}/salarios_top_10.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC