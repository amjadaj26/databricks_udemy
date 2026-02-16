# Databricks notebook source
# MAGIC %md
# MAGIC ### INJEST DATA FROM RAW TO BRONZE

# COMMAND ----------

# DBTITLE 1,INJEST DATA FROM RAW TO BRONZE
raw_df = spark.read.text("/Volumes/amjad_catalog/swift_mt940_recon/raw_data/MT940_sample.txt").withColumnRenamed("value","raw_line")

raw_df.write.format("delta").mode("overwrite").saveAsTable("amjad_catalog.swift_mt940_recon.mt940_bronze_table")

# COMMAND ----------

# MAGIC %md
# MAGIC