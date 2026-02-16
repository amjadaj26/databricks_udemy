# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### PARSE MT MESSGAES TO CSV FORMAT

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *


# COMMAND ----------

#raw_df = spark.read.text("/Volumes/amjad_catalog/swift_mt940_recon/raw_data/MT940_sample.txt").withColumnRenamed("value","raw_line")
raw_df = spark.read.table("amjad_catalog.swift_mt940_recon.mt940_bronze_table")
#display(raw_df)

#Parsing tag 20 and 25
#Create a new column called 'statement_ref' and 'account_no'
Statement_ref = raw_df.withColumn("statement_ref",regexp_extract(col("raw_line"),r":20:(.*)",1)).drop("raw_line").filter(col("statement_ref")!="")

Account_no = raw_df.withColumn("account_no",regexp_extract(col("raw_line"), r":25:(.*)", 1)).drop("raw_line").filter(col("account_no")!="")

#Opening, Current and Closing Balance

tag60F_pattern = r":60F:(C|D)(\d{6})([A-Z]{3})([0-9,]+)"

Opening_Bal = raw_df.withColumn("Opening_Bal",regexp_extract(col("raw_line"), tag60F_pattern, 4)).drop("raw_line").filter(col("Opening_Bal")!="")
Opening_Bal = Opening_Bal.withColumn("Opening_Bal",regexp_replace(col("Opening_Bal"),",","."))
Opening_Bal_Date = raw_df.withColumn("Opening_Bal_Date",regexp_extract(col("raw_line"), tag60F_pattern, 2)).drop("raw_line").filter(col("Opening_Bal_Date")!="")
Opening_Bal_Date = Opening_Bal_Date.withColumn("Opening_Bal_Date",try_to_date(col("Opening_Bal_Date"),"yyMMdd"))

tag62F_pattern = r":62F:(C|D)(\d{6})([A-Z]{3})([0-9,]+)"

Closing_Bal = raw_df.withColumn("Closing_Bal",regexp_extract(col("raw_line"), tag62F_pattern, 4)).drop("raw_line").filter(col("Closing_Bal")!="")
Closing_Bal = Closing_Bal.withColumn("Closing_Bal",regexp_replace(col("Closing_Bal"),",","."))
Closing_Bal_Date = raw_df.withColumn("Closing_Bal_Date",regexp_extract(col("raw_line"), tag62F_pattern, 2)).drop("raw_line").filter(col("Closing_Bal_Date")!="")

Closing_Bal_Date = Closing_Bal_Date.withColumn("Closing_Bal_Date",try_to_date(col("Closing_Bal_Date"),"yyMMdd"))

Currency = raw_df.withColumn("Currency",regexp_extract(col("raw_line"), tag60F_pattern, 3)).drop("raw_line").filter(col("Currency")!="")

#Joining the all the columns
silver_base = Statement_ref.join(Account_no).join(Opening_Bal).join(Closing_Bal).join(Closing_Bal_Date).join(Opening_Bal_Date).join(Currency)
display(silver_base)


# COMMAND ----------

#Parsing tag 61
#Creating new columns for Value_Date, Debit/Credit, Amount, Reference
tag61 = raw_df.filter(col("raw_line").contains(":61:"))
tag61 = tag61.withColumn("Value_Date",regexp_extract(col("raw_line"),r":61:(\d{6})",1))
tag61 = tag61.withColumn("Value_Date",try_to_date(col("Value_Date"),"yyMMdd"))
tag61 = tag61.withColumn(
    "Entry_Date",
    concat(
        regexp_extract(col("raw_line"), r":61:(\d{2})\d{4}(\d{4})(CR|DR)", 1),
        regexp_extract(col("raw_line"), r":61:(\d{2})\d{4}(\d{4})(CR|DR)", 2)
    )
)
tag61 = tag61.withColumn("Entry_Date",try_to_date(col("Entry_Date"),"yyMMdd"))
tag61 = tag61.withColumn("Debit/Credit",regexp_extract(col("raw_line"),r"(DR|CR)",1))
tag61 = tag61.withColumn("Amount",regexp_extract(col("raw_line"),r"(DR|CR)([0-9,]+)",2))
tag61 = tag61.withColumn("Reference",regexp_extract(col("raw_line"),r"NTRF(.*)",1))

tag61 = tag61.withColumn("Amount",regexp_replace(col("Amount"),",","."))
tag61 = tag61.drop("raw_line")

#Joining the silver_base with tag61
silver_base2 = silver_base.join(tag61)
display(silver_base2)

# COMMAND ----------

silver_base2.write.mode("overwrite").saveAsTable("amjad_catalog.swift_mt940_recon.mt940_silver_layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading Ledger file now as Silver layer table

# COMMAND ----------

ledger_raw = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/amjad_catalog/swift_mt940_recon/raw_data/Ledger_sample.csv")

ledger_raw = ledger_raw.withColumn("ledger_date",try_to_date(col("ledger_date"),"dd-MM-yyyy"))

#display(ledger_raw)
ledger_raw.write.mode("overwrite").saveAsTable("amjad_catalog.swift_mt940_recon.ledger_silver_layer")