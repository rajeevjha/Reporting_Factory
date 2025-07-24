import dlt
from pyspark.sql.functions import col

txn_path = "abfss://kyc-data@reprotingfactorydl.dfs.core.windows.net/finance/kyc/raw/paysim"

@dlt.table(name="raw_transactions", comment="Raw ingest from paysim payloads")
def raw_transactions():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", txn_path)
        .option("header", "true")
        .load(txn_path)
        .withColumn("ingest_source", col("_metadata.file_path"))    
    )
