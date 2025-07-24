import dlt
from pyspark.sql.functions import col

@dlt.table(name="validated_kyc_fraud")
@dlt.expect("valid_fraud_flag", "fraud_flag in ('Y', 'N')")
def validate_fraud_data():
    df = dlt.read("enriched_kyc_fraud")
    return df.filter(col("fraud_flag").isNotNull())

import dlt

@dlt.expect("valid_amount", "amount > 0")
@dlt.table(name="validated_transactions")
def apply_quality_checks():
    return dlt.read("raw_transactions")
