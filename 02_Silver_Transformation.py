# Databricks notebook source
bronze_data =spark.table('bronze_customer_churn')
bronze_data.display()

# COMMAND ----------

bronze_data.printSchema()
bronze_data.show(5)
bronze_data.count()

# COMMAND ----------

# for understanding of the data
bronze_data.select("customer_status").groupBy("customer_status").count().display()
bronze_data.select("churn_category").groupBy("churn_category").count().display()
bronze_data.select("churn_reason").groupBy("churn_reason").count().display()

# COMMAND ----------

#display the duplicates data
duplicate_rows = bronze_data.groupBy("customer_id").count().filter("count > 1")
bronze_data.join(duplicate_rows, "customer_id", "inner").display()

# COMMAND ----------

#remove the duplicates
from pyspark.sql.functions import col,count
silver_data = bronze_data.dropDuplicates(['customer_id'])

# COMMAND ----------

from pyspark.sql.functions import count, when,isnull
#Check nulls:
bronze_data.select([count(when(isnull(c), c)).alias(c) for c in bronze_data.columns]).display()

# COMMAND ----------

# DBTITLE 1,Cell 6
#handle  the null values
silver_data = bronze_data.fillna({
    'value_deal': 'unknown',
    'churn_category': 'unknow',
    'churn_reason': 'unknow',
    'internet_type': 'unknow',
  
})

silver_data.select([count(when(isnull(c), c)).alias(c) 
                    for c in silver_data.columns]).display()


# COMMAND ----------

#count the rows after drop the duplicate data
silver_data.count()


# COMMAND ----------

# DBTITLE 1,Cell 9
#CHECK THE DISTINCT  CATEGORY GROUPS
silver_data.select('gender').distinct().display()
silver_data.select('state').distinct().display()
silver_data.select('internet_type').distinct().display()
silver_data.select('contract').distinct().display()
silver_data.select('payment_method').distinct().display()



# COMMAND ----------

#Checking the columns for standardization
silver_data.columns

# COMMAND ----------

silver_data.printSchema()

# COMMAND ----------

#feature engineering
from pyspark.sql.functions import *
yes_no_cols = ['married','phone_service','multiple_lines','internet_service','online_security','online_backup','device_protection_plan','premium_support','streaming_tv','streaming_movies','streaming_music','unlimited_data','paperless_billing']

# COMMAND ----------

# DBTITLE 1,Cell 12
from pyspark.sql.functions import when

for c in yes_no_cols:
    silver_data = silver_data.withColumn(
        c,
        when(col(c) == "Yes", 1)
        .when(col(c) == "No", 0)
        .otherwise(0)
    )

# COMMAND ----------

silver_data.display()

# COMMAND ----------

#check the null values
silver_data.select([count(when(isnull(c), c)).alias(c) 
                    for c in silver_data.columns]).display()

# COMMAND ----------

# DBTITLE 1,Cell 16
#feature engineering
from pyspark.sql.functions import when

silver_data = silver_data.withColumn(
    "tenure_bucket",
    when(col("tenure_in_months") <= 6, "0-6 months")
    .when(col("tenure_in_months") <= 12, "7-12 months")
    .when(col("tenure_in_months") <= 24, "13-24 months")
    .otherwise("24+ months")
)

# COMMAND ----------

silver_data.display()

# COMMAND ----------

#“In the Silver layer, I removed duplicates, handled nulls, standardized categorical values, and encoded binary features using conditional logic to avoid casting errors and ensure ML compatibility.”

# COMMAND ----------

#saving the silver table into a delta table
silver_data.write.mode("overwrite").saveAsTable("silver_churns_data")

# COMMAND ----------

