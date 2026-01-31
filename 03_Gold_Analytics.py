# Databricks notebook source
#read the sivler data
Gold_data =spark.table('silver_churns_data')
display(Gold_data)

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql import functions as F
# EXECUTIVE SUMMARY TABLE
executive_summary = Gold_data.agg(
    F.count("*").alias("total_customers"),
    F.sum(F.when(F.col("customer_status") == "Stayed", 1).otherwise(0)).alias("retention_customers"),
    F.sum(F.when(F.col("customer_status") == "Churned", 1).otherwise(0)).alias("churned_customers"),
    F.round(
        (F.sum(F.when(F.col("customer_status") == "Joined", 1).otherwise(0)) / F.count("*")) * 100, 2
    ).alias("new_customers_rate_percent"),
    F.round(
        F.sum(F.when(F.col("customer_status") == "Churned", F.col("monthly_charge")).otherwise(0)), 2
    ).alias("monthly_revenue_lost")
)

executive_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customer_churn_catelog.gold.KPI_executive_summary")

# COMMAND ----------

#KPI's Summary
spark.table('customer_churn_catelog.gold.KPI_executive_summary').display()

# COMMAND ----------

# BUSINESS QUESTION:
#How many customers are churned?
from pyspark.sql import functions as F

total_count = Gold_data.count()
churn_percent = Gold_data.groupBy('customer_status') \
    .count() \
    .withColumn('percentage', F.round((F.col('count') / total_count) * 100, 2)) \
    .orderBy('count', ascending=False)
display(churn_percent)
#SAVE  IN GOLD TABLES
churn_percent.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customer_churn_catelog.gold.gold_churn_overview")

# COMMAND ----------

# DBTITLE 1,Cell 2
# BUSINESS QUESTION:
#which contract_type customers are most likely to churn

churned_df = Gold_data.filter(Gold_data.customer_status == "Churned")
total_churned = churned_df.count()

churn_contract_percent = churned_df.groupBy('contract') \
    .count() \
    .withColumn('percentage', F.round((F.col('count') / total_churned) * 100, 2))
display(churn_contract_percent)
#SAVE  IN GOLD TABLES
churn_contract_percent.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customer_churn_catelog.gold.gold_churn_contract_overview")


# COMMAND ----------

# DBTITLE 1,Cell 3
# BUSINESS QUESTION:
#which reason  customers are most likely to churning 
churned_df = Gold_data.filter(Gold_data.customer_status == "Churned")
total_churned = churned_df.count()

churn_reasons_df = churned_df.groupBy('churn_reason') \
    .count() \
    .withColumn('percentage', F.round((F.col('count') / total_churned) * 100, 2)) \
    .orderBy('percentage', ascending=False) \
    .limit(10)
display(churn_reasons_df)

#SAVE  IN GOLD TABLES
churn_reasons_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customer_churn_catelog.gold.gold_churn_reason_overview")


# COMMAND ----------

# BUSINESS QUESTION:
#which month do customers churn
churned_df = Gold_data.filter(F.col('customer_status') == "Churned")
total_churned = churned_df.count()

churn_month_percent = churned_df.groupBy('tenure_in_months') \
    .count() \
    .withColumn('percentage', F.round((F.col('count') / total_churned) * 100, 2)) \
    .orderBy('percentage', ascending=False)
display(churn_month_percent)

churn_month_percent.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("customer_churn_catelog.gold.gold_churn_mon_overview")

# COMMAND ----------

# DBTITLE 1,Cell 7
# BUSINESS QUESTION:
#which payment method customers more likely to churn, in percentage
churned_payment_method_df = Gold_data.filter(Gold_data.customer_status == "Churned")
total_churned = churned_payment_method_df.count()

churned_payment_percent_df = churned_payment_method_df.groupBy('payment_method') \
    .count() \
    .withColumn('percentage', F.round((F.col('count') / total_churned) * 100, 2)) \
    .orderBy('percentage', ascending=False)
display(churned_payment_percent_df)

#SAVE  IN GOLD TABLES
churned_payment_percent_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customer_churn_catelog.gold.gold_churn_payment_overview")

# COMMAND ----------

# BUSINESS QUESTION:
#which state customers more likey to churn in percentage
churned_state_df = Gold_data.filter(Gold_data.customer_status == "Churned")
total_churned = churned_state_df.count()

churned_state_percent_df = churned_state_df.groupBy('state') \
    .count() \
    .withColumn('percentage', F.round((F.col('count') / total_churned) * 100, 2)) \
    .orderBy('percentage', ascending=False)
display(churned_state_percent_df)

#SAVE  IN GOLD TABLES
churned_state_percent_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customer_churn_catelog.gold.gold_churn_state_overview")

# COMMAND ----------

#Business Question
# which Features are most likely to churn.
from pyspark.sql import functions as F

churned_df = Gold_data.filter(F.col('customer_status') == 'Churned')
total_churned = churned_df.count()

columns_to_check = [
    'online_security',
    'online_backup',
    'device_protection_plan',
    'premium_support',
    'unlimited_data'
]

churn_percent_list = []
for col in columns_to_check:
    df = churned_df.groupBy(col) \
        .count() \
        .withColumn('percentage', F.round((F.col('count') / total_churned) * 100, 2)) \
        .withColumnRenamed(col, 'feature') \
        .withColumn('feature_type', F.lit(col))
    churn_percent_list.append(df)

churn_features_df = churn_percent_list[0]
for df in churn_percent_list[1:]:
    churn_features_df = churn_features_df.unionByName(df)

display(churn_features_df.orderBy('feature_type', 'percentage', ascending=False))


#SAVE  IN GOLD TABLES
churn_features_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customer_churn_catelog.gold.gold_churn_features_overview")