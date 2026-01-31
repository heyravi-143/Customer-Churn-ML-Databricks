# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS customer_churn_catelog.bronze.raw_files;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Fix: Read Excel file from correct volume path
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/customer_churn_catelog/bronze/raw_files/customer_churn_data.csv")

# COMMAND ----------

# DBTITLE 1,Cell 3
df.write.format("delta").mode("overwrite").saveAsTable("bronze_customer_churn")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_customer_churn

# COMMAND ----------

