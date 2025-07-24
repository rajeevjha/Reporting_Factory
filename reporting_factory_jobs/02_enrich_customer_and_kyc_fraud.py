import dlt
from pyspark.sql.functions import col, when, expr, to_date, date_add, lit

@dlt.table(name="customer_enriched")
def create_customer_profiles():
    df = dlt.read("raw_transactions")
    # Create a pseudo-date column using '2020-01-01' as starting point
    df = df.withColumn("date", date_add(to_date(lit("2020-01-01")), col("step").cast("int")))
    df = df.withColumn("customer_id", col("nameOrig"))
    return df

@dlt.table(name="enriched_kyc_fraud")
def flag_fraud_transactions():
    df = dlt.read("raw_transactions")
    return df.withColumn("fraud_flag", when(col("isFraud") == 1, "Y").otherwise("N"))
