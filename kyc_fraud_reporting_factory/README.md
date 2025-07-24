
# KYC Fraud Detection Reporting Factory (Databricks + DLT + MLflow)

## Contents
- notebooks/model_training.py → Train and register model to MLflow
- notebooks/dlt_predict_fraud.py → Predict fraud using MLflow model in DLT
- kyc_fraud_pipeline.yaml → DLT pipeline config file

## Setup Instructions

1. Upload the `notebooks/` folder into your Databricks workspace.
2. Run `model_training.py` once to train and register a model in MLflow.
3. Promote the model to Production in MLflow UI.
4. Create a new DLT pipeline:
   - Click **Workflows > Delta Live Tables > Create Pipeline**
   - Use `kyc_fraud_pipeline.yaml` as configuration.
   - Select `dlt_predict_fraud.py` as the main notebook.
5. Start the DLT pipeline.
