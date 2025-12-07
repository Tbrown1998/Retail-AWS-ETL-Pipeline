# incremental_auto_compaction.py
# Auto-detect processed partitions and compact them into gold/date=YYYY-MM-DD/
# Usage (Glue job args):
#   --JOB_NAME        <Glue job name>
#   --processed_path  s3://<bucket>/processed/
#   --gold_path       s3://<bucket>/gold/fact_sales/
#   --audit_path      s3://<bucket>/audit/
#   --max_partitions  optional int, max partitions to process in one run (default 50)
#   --reprocess       optional "true" to recompact partitions already present in gold (default false)
#   --force_dates     optional comma-separated YYYY-MM-DD list to force process those dates (overrides detection)
#
# Behavior:
#  - Finds processed partitions of the form: processed/.../date=YYYY-MM-DD/
#  - If force_dates provided: processes exactly those dates (if found in processed)
#  - Else computes partitions_to_process = processed_dates - gold_dates (unless reprocess=true)
#  - Processes partitions in ascending date order (oldest first)
#  - Writes each partition to gold_path/date=YYYY-MM-DD/ (overwrite)
#  - Emits per-partition audit JSON to audit_path/gold_compaction/date=YYYY-MM-DD/metrics.json
#  - Job is idempotent: re-running same date will overwrite the partition

import sys
import json
import re
import boto3
from datetime import datetime
from urllib.parse import urlparse

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, coalesce, md5, concat_ws, current_timestamp, row_number, desc

# ---------------------------
# Args
# ---------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "processed_path",
        "gold_path",
        "audit_path"
    ]  # optional args fetched below by .get with defaults
)

JOB_NAME = args["JOB_NAME"]
processed_path = args["processed_path"].rstrip("/") + "/"
gold_path = args["gold_path"].rstrip("/") + "/"
audit_path = args["audit_path"].rstrip("/") + "/"
#force_dates = 2024-10-16


# optional args via getResolvedOptions would raise if listed; so parse from sys.argv manual fallback
def get_optional(arg_name, default=None):
    prefix = f"--{arg_name}="
    for a in sys.argv:
        if a.startswith(prefix):
            return a.split("=", 1)[1]
    return default

max_partitions = int(get_optional("max_partitions", "10"))
reprocess_flag = get_optional("reprocess", "false").lower() == "true"
force_dates_arg = get_optional("force_dates", "").strip()  # comma separated
force_dates = [d.strip() for d in force_dates_arg.split(",") if d.strip()] if force_dates_arg else []

# ---------------------------
# Init clients & Spark
# ---------------------------
s3 = boto3.client("s3")
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

print(f"[INFO] Starting auto compaction job: {JOB_NAME}")
print(f"[INFO] processed_path={processed_path}")
print(f"[INFO] gold_path={gold_path}")
print(f"[INFO] audit_path={audit_path}")
print(f"[INFO] max_partitions={max_partitions} reprocess={reprocess_flag} force_dates={force_dates}")

# ---------------------------
# Helpers: list partitions under a prefix like 'processed_path'
# ---------------------------
def list_partition_dates(s3_path):
    """
    Return sorted list of date strings (YYYY-MM-DD) found under s3_path matching 'date=YYYY-MM-DD/'.
    Uses S3 list_objects_v2 with Delimiter='/' to get common prefixes if possible.
    """
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    paginator = s3.get_paginator("list_objects_v2")
    operation_parameters = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
    dates = set()

    for page in paginator.paginate(**operation_parameters):
        # Look into CommonPrefixes first (folders)
        cps = page.get("CommonPrefixes", [])
        for cp in cps:
            p = cp.get("Prefix", "")
            # find date=YYYY-MM-DD/ in p
            m = re.search(r"date=(\\d{4}-\\d{2}-\\d{2})/", p)
            if m:
                dates.add(m.group(1))
        # Additionally scan object keys if no CommonPrefixes discovered
        for obj in page.get("Contents", []):
            key = obj.get("Key", "")
            m = re.search(r"date=(\\d{4}-\\d{2}-\\d{2})/", key)
            if m:
                dates.add(m.group(1))

    return sorted(list(dates))

def partition_exists(s3_path, date_str):
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    key_prefix = f"{prefix}date={date_str}/"
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=key_prefix, MaxKeys=1)
    return "Contents" in resp

# ---------------------------
# Find processed & gold partitions
# ---------------------------
processed_dates = list_partition_dates(processed_path)
print(f"[INFO] Found processed partitions: {processed_dates}")

gold_dates = list_partition_dates(gold_path)
print(f"[INFO] Found gold partitions: {gold_dates}")

# If force_dates provided, validate they exist in processed and use only those
if force_dates:
    valid_force = [d for d in force_dates if d in processed_dates]
    missing_force = [d for d in force_dates if d not in processed_dates]
    if missing_force:
        print(f"[WARN] Force dates not found in processed: {missing_force} (they will be skipped)")
    partitions_to_process = sorted(valid_force)
else:
    if reprocess_flag:
        # process everything found under processed (you may limit by max_partitions)
        partitions_to_process = processed_dates
    else:
        # default: process dates present in processed but not yet present in gold
        partitions_to_process = sorted([d for d in processed_dates if d not in gold_dates])

# Respect max_partitions limit
if len(partitions_to_process) > max_partitions:
    print(f"[WARN] {len(partitions_to_process)} partitions found to process but max_partitions={max_partitions}. Truncating.")
    partitions_to_process = partitions_to_process[:max_partitions]

print(f"[INFO] Partitions selected for processing: {partitions_to_process}")

if not partitions_to_process:
    print("[INFO] Nothing to process. Exiting.")
    job.commit()
    sys.exit(0)

# ---------------------------
# Core per-partition compaction function
# ---------------------------
def compact_partition(date_str):
    input_partition_path = f"{processed_path}date={date_str}/"
    output_partition_path = f"{gold_path}date={date_str}/"
    audit_metrics_key = f"gold_compaction/date={date_str}/metrics.json"

    print(f"[INFO] Processing partition date={date_str}")
    print(f"[INFO] Input: {input_partition_path}")
    print(f"[INFO] Output: {output_partition_path}")

    # Validate existence
    parsed_in = urlparse(input_partition_path)
    bucket_in = parsed_in.netloc
    prefix_in = parsed_in.path.lstrip("/")
    if not prefix_in.endswith("/"):
        prefix_in = prefix_in + "/"
    resp = s3.list_objects_v2(Bucket=bucket_in, Prefix=prefix_in, MaxKeys=1)
    if "Contents" not in resp:
        print(f"[WARN] No objects found for {input_partition_path}. Skipping.")
        return {"date": date_str, "status": "no_input"}

    # Read partition safely
    try:
        df = spark.read.option("mergeSchema", "true").parquet(input_partition_path)
    except Exception as e:
        print(f"[ERROR] Failed reading partition {input_partition_path}: {e}")
        return {"date": date_str, "status": "read_failed", "error": str(e)}

    # Ensure expected columns exist - add safe defaults
    expected_cols = [
        "transaction_id", "store_id", "timestamp", "item_id",
        "item_category", "quantity", "unit_price", "revenue",
        "payment_method", "customer_id", "ingest_run_id", "source_file",
        "ingest_ts", "date"
    ]
    for c in expected_cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None).cast(StringType()))

    # Normalize numeric-ish strings (defensive)
    df = df.withColumn("quantity", F.regexp_replace(col("quantity").cast(StringType()), "[^0-9-]", "").cast("long"))
    df = df.withColumn("unit_price", F.regexp_replace(col("unit_price").cast(StringType()), "[^0-9.\\-()]", "").cast("double"))
    df = df.withColumn("revenue", F.regexp_replace(col("revenue").cast(StringType()), "[^0-9.\\-()]", "").cast("double"))

    # Compute row_hash
    hash_cols = ["store_id", "timestamp", "item_id", "item_category", "quantity", "unit_price", "revenue", "payment_method", "customer_id"]
    concat_expr = concat_ws("||", *[coalesce(col(c).cast(StringType()), lit("")) for c in hash_cols])
    df = df.withColumn("row_hash", md5(concat_expr))

    # ingest_ts fallback to compaction time if missing
    df = df.withColumn("ingest_ts_parsed", F.to_timestamp(col("ingest_ts")))
    df = df.withColumn("compaction_ts", current_timestamp())
    df = df.withColumn("ingest_ts_f", coalesce(col("ingest_ts_parsed"), col("compaction_ts")))

    # Deduplicate by transaction_id keeping latest ingest_ts_f
    w = Window.partitionBy("transaction_id").orderBy(desc("ingest_ts_f"))
    df_dedup = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn", "ingest_ts_parsed", "ingest_ts_f", "compaction_ts")

    # Metrics
    try:
        total_rows = int(df.count())
        rows_after_dedup = int(df_dedup.count())
    except Exception as e:
        print(f"[ERROR] Counting rows failed for date={date_str}: {e}")
        return {"date": date_str, "status": "count_failed", "error": str(e)}

    duplicates_removed = total_rows - rows_after_dedup
    null_timestamp = int(df_dedup.filter(col("timestamp").isNull()).count())
    null_store = int(df_dedup.filter(col("store_id").isNull()).count())
    dq_balance_issues = int(df_dedup.filter(
        (col("revenue").isNotNull()) &
        (col("quantity").isNotNull()) &
        (F.abs(col("revenue") - (col("quantity") * col("unit_price"))) > 0.01)
    ).count())

    metrics = {
        "target_date": date_str,
        "input_partition": input_partition_path,
        "output_partition": output_partition_path,
        "total_rows_in_source_partition": total_rows,
        "rows_after_dedup": rows_after_dedup,
        "duplicate_rows_removed": duplicates_removed,
        "null_timestamp": null_timestamp,
        "null_store": null_store,
        "dq_balance_issues": dq_balance_issues,
        "processed_at_utc": datetime.utcnow().isoformat()
    }

    # Write compacted partition (overwrite directory for this partition)
    coalesce_files = 4
    try:
        df_out = df_dedup.orderBy(col("transaction_id")).coalesce(coalesce_files)
        df_out.write.mode("overwrite").parquet(output_partition_path)
        metrics["status"] = "written"
        print(f"[INFO] Wrote gold partition for date={date_str} to {output_partition_path}")
    except Exception as e:
        metrics["status"] = "write_failed"
        metrics["error"] = str(e)
        print(f"[ERROR] Failed writing partition {output_partition_path}: {e}")

    # Write metrics to audit S3
    try:
        parsed_audit = urlparse(audit_path)
        audit_bucket = parsed_audit.netloc
        audit_prefix = parsed_audit.path.lstrip("/").rstrip("/")
        metrics_s3_key = f"{audit_prefix}/gold_compaction/date={date_str}/metrics.json" if audit_prefix else f"gold_compaction/date={date_str}/metrics.json"
        s3.put_object(Bucket=audit_bucket, Key=metrics_s3_key, Body=json.dumps(metrics).encode("utf-8"))
        print(f"[INFO] Wrote metrics to s3://{audit_bucket}/{metrics_s3_key}")
    except Exception as e:
        print(f"[ERROR] Failed to write audit metrics for date={date_str}: {e}")

    return metrics

# ---------------------------
# Run compaction for each partition
# ---------------------------
results = []
for d in partitions_to_process:
    res = compact_partition(d)
    results.append(res)

# ---------------------------
# Summary write
# ---------------------------
summary = {
    "job_name": JOB_NAME,
    "run_ts_utc": datetime.utcnow().isoformat(),
    "processed_partitions_count": len(partitions_to_process),
    "results": results
}

# attempt to write a run-level summary to audit path
try:
    parsed_audit = urlparse(audit_path)
    audit_bucket = parsed_audit.netloc
    audit_prefix = parsed_audit.path.lstrip("/").rstrip("/")
    summary_key = f"{audit_prefix}/gold_compaction/last_run_summary.json" if audit_prefix else "gold_compaction/last_run_summary.json"
    s3.put_object(Bucket=audit_bucket, Key=summary_key, Body=json.dumps(summary).encode("utf-8"))
    print(f"[INFO] Wrote summary to s3://{audit_bucket}/{summary_key}")
except Exception as e:
    print(f"[WARN] Failed to write run summary: {e}")


# -----------------------------------------
# OPTIONAL: Auto-start Glue crawler after compaction
# -----------------------------------------
crawler_name = get_optional("crawler_name", None)

if crawler_name:
    glue_client = boto3.client("glue")

    try:
        print(f"[INFO] Starting crawler: {crawler_name}")
        glue_client.start_crawler(Name=crawler_name)
        print(f"[INFO] Crawler {crawler_name} started successfully.")

    except glue_client.exceptions.CrawlerRunningException:
        print(f"[WARN] Crawler {crawler_name} is already running. Skipping start.")

    except Exception as e:
        print(f"[ERROR] Failed to start crawler {crawler_name}: {str(e)}")


job.commit()
print("[INFO] Auto compaction job finished.")
