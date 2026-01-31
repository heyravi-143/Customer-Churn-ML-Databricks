# Databricks notebook source
silver_data = spark.table("silver_churns_data")
display(silver_data)

# COMMAND ----------

from pyspark.sql.functions import when, col

silver_data = spark.table("silver_churns_data")

silver_data = silver_data.withColumn(
    "label",
    when(col("customer_status") == "Churned", 1)
    .otherwise(0)
)

# COMMAND ----------

display(silver_data)

# COMMAND ----------

#Feature lists
numerical_cols = [
    "age",
    "number_of_referrals",
    "tenure_in_months",
    "monthly_charge",
    "total_revenue"
]

categorical_cols = [
    "gender",
    "married",
    "state",
    "internet_type",
    "online_security",
    "online_backup",
    "device_protection_plan",
    "premium_support",
    "contract",
    "payment_method"
]

# COMMAND ----------

#Encode ALL categoricals at once
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

indexer = StringIndexer(
    inputCols=categorical_cols,
    outputCols=[f"{c}_index" for c in categorical_cols],
    handleInvalid="keep"
)

df_indexed = indexer.fit(silver_data).transform(silver_data)

encoder = OneHotEncoder(
    inputCols=[f"{c}_index" for c in categorical_cols],
    outputCols=[f"{c}_encoded" for c in categorical_cols]
)

df_encoded = encoder.fit(df_indexed).transform(df_indexed)

# COMMAND ----------

#Assemble features
assembler = VectorAssembler(
    inputCols=numerical_cols + [f"{c}_encoded" for c in categorical_cols],
    outputCol="features"
)

df_final = assembler.transform(df_encoded)


# COMMAND ----------

#Train model
from pyspark.ml.classification import LogisticRegression

train_df, test_df = df_final.randomSplit([0.8, 0.2], seed=42)

lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(train_df)

# COMMAND ----------

#Predict & evaluate
predictions = model.transform(test_df)

predictions.select(
    "customer_id", "label", "prediction", "probability"
).show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.ml.functions import vector_to_array
predictions_with_prob = predictions.withColumn(
    "churn_probability",
    vector_to_array(col("probability"))[1]   # index 1 = churn
)

# COMMAND ----------

from pyspark.sql.functions import when

predictions_with_risk = predictions_with_prob.withColumn(
    "churn_risk",
    when(col("churn_probability") >= 0.80, "High")
    .when(col("churn_probability") >= 0.50, "Medium")
    .otherwise("Low")
)

# COMMAND ----------

# Evaluate Accuracy
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"Test Accuracy: {accuracy*100:.2f}%")


# COMMAND ----------

#  Feature Importance (Executive-friendly)
import pandas as pd

coef = model.coefficients.toArray().flatten()

feature_importance = pd.DataFrame({
    "Feature": numerical_cols,
    "Impact": coef[:len(numerical_cols)]
}).sort_values("Impact", ascending=False)

feature_importance


# COMMAND ----------

#  Feature Importance (Executive-friendly)
import pandas as pd

coef = model.coefficients.toArray().flatten()

feature_importance = pd.DataFrame({
    "Feature": categorical_cols,
    "Impact": coef[:len(categorical_cols)]
}).sort_values("Impact", ascending=False)

feature_importance

# COMMAND ----------

# 9️⃣ Save Predictions to Gold Table
predictions_to_save = predictions_with_risk.select(
    "customer_id",
    "label",
    "prediction",
    "probability",
    "tenure_bucket",
    "contract",
    "state",
    "monthly_charge",
    "total_revenue",
    "churn_probability",
    "churn_risk"
)

predictions_to_save.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_churns_predictions")
print("✅ Gold table saved: gold_churn_predictions")

# COMMAND ----------

