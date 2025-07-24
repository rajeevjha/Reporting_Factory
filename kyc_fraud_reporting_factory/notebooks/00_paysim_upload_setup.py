
# Databricks notebook source
# MAGIC %md
# MAGIC ## PaySim Upload & Table Registration
# MAGIC This notebook uploads the PaySim CSV file, reads it into a DataFrame, and creates a Delta table for model training.

# COMMAND ----------
dbfs_path = "abfss://kyc-data@reprotingfactorydl.dfs.core.windows.net/finance/kyc/raw/paysim.csv"
table_name = "finance.kyc_ml.paysim_cleaned"

# Step 1: Upload the dataset to DBFS using UI or CLI
# If already uploaded, skip this step.

# COMMAND ----------
# Step 2: Load CSV into DataFrame
df = spark.read.option("header", True).csv(dbfs_path)

# Convert column types as needed
from pyspark.sql.functions import col

df = df.selectExpr(
    "cast(step as int) step",
    "cast(amount as double) amount",
    "cast(oldbalanceOrg as double) oldbalanceOrg",
    "cast(newbalanceOrig as double) newbalanceOrig",
    "cast(oldbalanceDest as double) oldbalanceDest",
    "cast(newbalanceDest as double) newbalanceDest",
    "cast(isFraud as int) isFraud"
)

# COMMAND ----------
# Step 3: Save as Delta table for model training
df.write.mode("overwrite").format("delta").saveAsTable(table_name)

print(f"✅ Table `{table_name}` is ready for use in model_training.py")
