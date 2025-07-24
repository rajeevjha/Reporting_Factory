# Databricks notebook source
# MAGIC %md
# MAGIC ## Model Training: KYC Fraud Detection (PaySim)

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
import mlflow
import mlflow.spark

# Load and prepare training data
data = spark.read.table("finance.kyc_ml.paysim_cleaned")  # Update table path as needed

features = ["step", "amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"]
assembler = VectorAssembler(inputCols=features, outputCol="features")
train_df = assembler.transform(data).select("features", "isFraud")

# Convert DenseVector to list for input_example
input_example = train_df.limit(5).toPandas()
input_example['features'] = input_example['features'].apply(lambda x: x.toArray().tolist())

# Train model
lr = LogisticRegression(featuresCol="features", labelCol="isFraud")
model = lr.fit(train_df)

# Log model to MLflow
with mlflow.start_run() as run:
    mlflow.spark.log_model(model, "model", input_example=input_example)
    mlflow.set_tag("use_case", "kyc_fraud_detection")
    mlflow.register_model(f"runs:/{run.info.run_id}/model", "kyc_fraud_model")