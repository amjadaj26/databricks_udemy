# Databricks notebook source
# MAGIC %md
# MAGIC ### **Volumes**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME amjad_catalog.bronze_layer.bronze_volumn;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`/Volumes/amjad_catalog/bronze_layer/bronze_volumn/fact_sales.csv`

# COMMAND ----------

dbutils.help()

# COMMAND ----------

all_items = dbutils.fs.ls('/Volumes/amjad_catalog/bronze_layer/bronze_volumn/fact_sales.csv')
file_name = [i.name for i in all_items]
file_name

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

df = spark.read.format('csv').option('header','true').load('/Volumes/amjad_catalog/bronze_layer/bronze_volumn/')
df.printSchema()
display(df)

# COMMAND ----------

df = df.withColumn("name", upper(col("name")))
df = df.withColumn("domain", split(col("email"),"@")[1])
df = df.withColumn("processDate",current_timestamp())
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME amjad_catalog.silver_layer.silver_volumn;

# COMMAND ----------

if spark.catalog.tableExists("amjad_catalog.silver_layer.silver_volume"):
    dlt_obj = DeltaTable.forName(spark,"amjad_catalog.silver_layer.silver_volumn")
    dlt_obj.alias("trg").merge(df.alias("src"),"trg.customer_id==src.customer_id")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df.write.format("delta").mode("append").saveAsTable("amjad_catalog.silver_layer.silver_volume")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from amjad_catalog.silver_layer.silver_volumn