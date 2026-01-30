# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME amjad_catalog.bronze_layer.autoloader_work

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.schemaLocation","/Volumes/amjad_catalog/bronze_layer/autoloader_work/destination/checkpoint/").option("checkpointLocation","/Volumes/amjad_catalog/bronze_layer/autoloader_work/destination/checkpoint" )\
  .option("cloudFiles.schemaEvolutionMode", "rescue")\
  .load("/Volumes/amjad_catalog/bronze_layer/autoloader_work/raw")

  

# COMMAND ----------

df.writeStream.format("delta").option("checkpointLocation", "/Volumes/amjad_catalog/bronze_layer/autoloader_work/destination/checkpoint")\
    .outputMode("append").trigger(once=True).start("/Volumes/amjad_catalog/bronze_layer/autoloader_work/destination/data")


# COMMAND ----------

df = spark.read.format("delta").load("/Volumes/amjad_catalog/bronze_layer/autoloader_work/destination/data")
display(df)