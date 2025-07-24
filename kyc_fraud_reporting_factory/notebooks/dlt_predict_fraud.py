
# Databricks notebook source
# MAGIC %md
# MAGIC ## DLT: Predict Fraud with MLflow Pre-trained Model


import dlt
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors, VectorUDT
import mlflow

feature_cols = ["step", "amount", "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"]

def to_vector(*cols):
    return Vectors.dense(cols)

vector_udf = udf(to_vector, VectorUDT())

model_uri = "models:/kyc_fraud_model/1"
model = mlflow.pyfunc.spark_udf(spark, model_uri, result_type='double')

@dlt.table(name="kyc_fraud_predictions")
def predict_kyc_fraud():
    df = dlt.read("validated_data").select(*feature_cols, "isFraud").na.drop()
    df = df.withColumn("features", vector_udf(*[F.col(c) for c in feature_cols]))
    df = df.withColumn("prediction", model(F.col("features")))
    return df.select(*feature_cols, "isFraud", "features", "prediction")
