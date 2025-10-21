# Databricks notebook source
from pyspark.sql.types import StructField, StructType, StringType, LongType,IntegerType, DateType, DecimalType
myManualSchema = StructType([
  StructField("work_year", IntegerType(), True),
  StructField("experience_level", StringType(),True),
  StructField('employment_type', StringType(),True),
  StructField("job_title", StringType(), True),
  StructField("salary", DecimalType(10,2),True),
  StructField("salary_currency", StringType(),True),
  StructField('salary_in_usd', DecimalType(10,2),True),
  StructField('employee_residence', StringType(),True),
  StructField("remote_ratio", IntegerType(), False),
  StructField("company_location", StringType(),True),
  StructField('company_size', StringType(),True)
  ]                                                   
                            )

# COMMAND ----------

df =(
    spark
    .read 
    .schema(myManualSchema)
    .option("header",True)
    .csv("s3://galedbucket/Data/databricks/salaries.csv")
)
#df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.describe()

# COMMAND ----------

#Cantidad de nulos por columna
from pyspark.sql.functions import isnull, col, isnan
nullldic = {}
for campo in df.columns:
    nulls = df.filter(col(campo).isNull()).count()
    nullldic[campo] = nulls
print(nullldic)


# COMMAND ----------

from pyspark.sql.functions import lower
strings = ['experience_level','employment_type','job_title','salary_currency','employee_residence','company_location','company_size']
for nombre in strings:
    df = df.withColumn(nombre,lower(col(nombre)))
#display(df)

# COMMAND ----------

#Salarios promedio por nivel de experiencia
from pyspark.sql.functions import avg
df1 = df.groupBy('experience_level').agg(avg("salary").alias("salario_promedio"))
df1.show()

#Guardamos en un formato csv en amazon s3
(
df1
.write
.format("csv")
.mode("overwrite")
.option("header",True)
.save("s3a://galedbucket/Data/databricks/salario_promedio.csv")
)

# COMMAND ----------

#Salario promedio por tipo de trabajo
df2 = df.groupBy('job_title').agg((avg("salary")).alias("salario_promedio_trabajos"))
df2.display(20)

(
df2
.write
.format("csv")
.mode("overwrite")
.option("header",True)
.save("s3a://galedbucket/Data/databricks/salario_promedio_trabajo.csv")
)


# COMMAND ----------

#Salario Maximo 
df3 = df.select(['job_title','salary']).sort('salary', ascending=False)
dfmax = df3.limit(1)
dfmax.show()
(
dfmax
.write
.format("csv")
.mode("overwrite")
.option("header",True)
.save("s3a://galedbucket/Data/databricks/salario_maximo.csv")
)

# COMMAND ----------

#Salario Minimo
df3 = df.select(['job_title','salary']).sort('salary', ascending=True)
dfmin = df3.limit(1)
dfmin.show()
(
dfmin
.write
.format("csv")
.mode("overwrite")
.option("header",True)
.save("s3a://galedbucket/Data/databricks/salario_minimo.csv")
)

# COMMAND ----------

#10 Salarios mas altos con descripcion
df10 = df.select(['job_title','experience_level','company_location','salary']).sort('salary', ascending=False)
dfmax_sal = df10.limit(10)
dfmax_sal.show()

(
dfmax_sal
.write
.format("csv")
.mode("overwrite")
.option("header",True)
.save("s3a://galedbucket/Data/databricks/salarios_top_10.csv")
)


# COMMAND ----------

# MAGIC %md
# MAGIC