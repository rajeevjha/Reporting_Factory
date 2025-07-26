import json
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.utils import AnalysisException

# Path to metadata file (DBFS or ADLS)
metadata_path = "abfss://kyc-data@reprotingfactorydl.dfs.core.windows.net/finance/kyc/metadata/report_definitions.json"

# Load metadata
try:
    metadata_df = spark.read.option("multiline", "true").json(metadata_path)
    reports = metadata_df.toJSON().map(json.loads).collect()
except Exception as e:
    raise Exception(f"Failed to load metadata file: {metadata_path}. Error: {e}")

# Loop through each report definition
for report in reports:
    report_name = report["report_name"]
    source_table = report.get("source", "finance.kyc_ml.customer_enriched")
    output_format = report.get("format", "delta")

    try:
        df = spark.table(source_table)
    except AnalysisException:
        raise Exception(f"Source table '{source_table}' not found.")

    # Apply filters if specified
    if "filters" in report and report["filters"]:
        df = df.filter(report["filters"])

    # Apply groupings and aggregations
    if "dimensions" in report and "measures" in report:
        grouped_df = df.groupBy(*report["dimensions"]).agg(
            *[eval(m) if "F." in m else eval(f"F.{m}") for m in report["measures"]]
        )
    else:
        grouped_df = df

    # Save as table
    grouped_df.write.format(output_format).mode("overwrite").option("mergeSchema", "true").saveAsTable(f"finance.kyc_ml.{report_name}")

    # Unity Catalog table tags (optional but helpful for lineage)
    lineage_tags = {
        "lineage.source": source_table,
        "lineage.transformation": "report_generation_job",
        "lineage.created_by": "job_run",
        "lineage.certified": str(report.get("certify", False)).lower(),
        "lineage.refresh_time": datetime.now().isoformat(),
        "lineage.pipeline_version": "v1.0"
    }

    for k, v in lineage_tags.items():
        spark.sql(f"ALTER TABLE finance.kyc_ml.{report_name} SET TBLPROPERTIES ('{k}' = '{v}')")

print("All reports generated successfully.")