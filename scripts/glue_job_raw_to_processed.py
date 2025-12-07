
import sys
import boto3
import csv
from io import StringIO
import datetime

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *


# 1. Read Job Parameters

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "s3_input_path",
        "s3_output_path",
        "ingest_run_id",
        "source_file",
        "original_key",
        "sns_topic_arn"
    ]
)

input_path    = args["s3_input_path"]
output_path   = args["s3_output_path"]
ingest_run_id = args["ingest_run_id"]
source_file   = args["source_file"]
original_key  = args["original_key"]
sns_topic     = args["sns_topic_arn"]

bucket_name   = input_path.split("/")[2]
validated_key = "/".join(input_path.split("/")[3:])

dq_json_path = output_path.replace("processed", "rejected/data_quality/json")
dq_csv_path  = output_path.replace("processed", "rejected/data_quality/csv")

sns_client = boto3.client("sns")
s3 = boto3.client("s3")



# 2. Initialize Glue & Spark

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)




# UTILITY: Move file safely to rejected/system

def move_to_system_reject(reason_text):
    reject_key = f"rejected/system/{source_file}"
    reason_key = f"{reject_key}_reason.json"

    try:
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": validated_key},
            Key=reject_key)
        s3.delete_object(Bucket=bucket_name, Key=validated_key)
    except:
        pass

    s3.put_object(
        Bucket=bucket_name,
        Key=reason_key,
        Body=str(reason_text)
    )

    if sns_topic:
        sns_client.publish(
            TopicArn=sns_topic,
            Subject="GLUE SYSTEM FAILURE",
            Message=str(reason_text)
        )




# MAIN PROCESSING LOGIC (Wrapped in try/except for atomicity)

try:
    
    # 3. Read file as raw
    
    raw_df = spark.read.text(input_path)

    clean_df = raw_df.withColumn(
        "value",
        regexp_replace("value", "[\\uFEFF\\u200B\\u00A0]", "")
    ).filter(trim(col("value")) != "")


    
    # 4. Detect delimiter
 
    sample_lines = [r.value for r in clean_df.limit(20).collect()]
    sample_text = "\n".join(sample_lines)

    detected = None
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters=";,|\t")
        detected = dialect.delimiter
    except:
        pass

    if detected is None:
        candidates = [",", ";", "|", "\t"]
        counts = {c: sample_text.count(c) for c in candidates}

        # Safe fallback detection
        detected = sorted(counts.items(), key=lambda x: x[1], reverse=True)[0][0]

    if counts[detected] == 0:
        detected = ","

    delimiter = detected
    print(f"Detected delimiter: {delimiter}")


   
    # 5. Extract header row and normalize
    
    header_line = clean_df.first()["value"]
    header_raw_cols = header_line.split(delimiter)

    def normalize_header(colname):
        c = colname.lower()
        c = c.replace(" ", "_").replace("-", "_")
        c = "".join([ch for ch in c if ch.isalnum() or ch == "_"])
        return c

    normalized = [normalize_header(x) for x in header_raw_cols]

    synonyms = {
        "transactionid": "transaction_id",
        "transid": "transaction_id",
        "txn_id": "transaction_id",

        "storeid": "store_id",
        "shop_id": "store_id",

        "itemid": "item_id",
        "product_id": "item_id",

        "qty": "quantity",
        "quantitysold": "quantity",

        "unitprice": "unit_price",
        "price": "unit_price",

        "revenueamount": "revenue",
        "amount": "revenue",
        "revenue": "revenue",
    }

    final_headers = [synonyms.get(h, h) for h in normalized]


   
    # 6. Required columns
   
    REQUIRED = {
        "transaction_id",
        "store_id",
        "timestamp",
        "item_id",
        "quantity",
        "unit_price",
        "revenue"
    }

    index_map = {final_headers[i]: i for i in range(len(final_headers))}


   
    # 7. Split rows (skip header)
  
    data_df = clean_df.filter(col("value") != header_line)
    split_df = data_df.withColumn("cols", split(col("value"), delimiter))


    
    # 8. Extract mapped columns
    
    def extr(name):
        idx = index_map.get(name, None)
        if idx is None:
            return lit(None)
        return col("cols")[idx]

    df_extracted = split_df.select(
        extr("transaction_id").alias("transaction_id"),
        extr("store_id").alias("store_id"),
        extr("timestamp").alias("timestamp_raw"),
        extr("item_id").alias("item_id"),
        extr("item_category").alias("item_category"),
        extr("quantity").alias("quantity"),
        extr("unit_price").alias("unit_price"),
        extr("revenue").alias("revenue"),
        extr("payment_method").alias("payment_method"),
        extr("customer_id").alias("customer_id"),
        col("value").alias("raw_row")
    )


    
    # 9. Structural rejects
    
    missing_req_cond = (
        col("transaction_id").isNull() |
        col("store_id").isNull() |
        col("timestamp_raw").isNull() |
        col("item_id").isNull() |
        col("quantity").isNull() |
        col("unit_price").isNull() |
        col("revenue").isNull()
    )

    struct_rejects = df_extracted.filter(missing_req_cond) \
        .withColumn("reject_reason", lit("MISSING_REQUIRED_COLUMN"))

    df_struct_good = df_extracted.filter(~missing_req_cond)


    
    # 10. Hardened Multi-format timestamp parsing
    
    timestamp_patterns = [
        (r"^\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{2}$", "yyyy-MM-dd H:mm:ss"),
        (r"^\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}$", "yyyy-MM-dd H:mm"),

        (r"^\d{4}/\d{2}/\d{2} \d{1,2}:\d{2}:\d{2}$", "yyyy/MM/dd H:mm:ss"),
        (r"^\d{4}/\d{2}/\d{2} \d{1,2}:\d{2}$", "yyyy/MM/dd H:mm"),

        (r"^\d{2}/\d{2}/\d{4} \d{1,2}:\d{2}:\d{2}$", "MM/dd/yyyy H:mm:ss"),
        (r"^\d{2}/\d{2}/\d{4} \d{1,2}:\d{2}$", "MM/dd/yyyy H:mm"),

        (r"^\d{2}/\d{2}/\d{4}$", "MM/dd/yyyy"),
        (r"^\d{4}-\d{2}-\d{2}$", "yyyy-MM-dd"),
        (r"^\d{4}/\d{2}/\d{2}$", "yyyy/MM/dd"),

        (r"^\d{8} \d{6}$", "yyyyMMdd HHmmss"),
        (r"^\d{8}$", "yyyyMMdd"),
    ]

    parsed_col = lit(None)
    for pattern, fmt in timestamp_patterns:
        parsed_col = coalesce(
            parsed_col,
            when(col("timestamp_raw").rlike(pattern),
                 to_timestamp(col("timestamp_raw"), fmt))
        )

    df_struct_good = df_struct_good.withColumn("timestamp_parsed", parsed_col)

    timestamp_invalid = df_struct_good.filter(col("timestamp_parsed").isNull()) \
        .withColumn("reject_reason", lit("INVALID_TIMESTAMP_FORMAT"))

    df_struct_good = df_struct_good.filter(col("timestamp_parsed").isNotNull())


    
    # 11. Clean numeric fields
    
    def clean_currency(df, name):
        df = df.withColumn(name, regexp_replace(col(name), "[^0-9()\\.-]", ""))
        df = df.withColumn(name, regexp_replace(col(name), "[(]", "-"))
        df = df.withColumn(name, regexp_replace(col(name), "[)]", ""))
        return df.withColumn(name, col(name).cast("double"))

    df_struct_good = clean_currency(df_struct_good, "unit_price")
    df_struct_good = clean_currency(df_struct_good, "revenue")
    df_struct_good = df_struct_good.withColumn("quantity", regexp_replace(col("quantity"), "[^0-9-]", "").cast("int"))


    
    # 12. Add metadata
    
    df_struct_good = df_struct_good.withColumn("timestamp", col("timestamp_parsed"))
    df_struct_good = df_struct_good.withColumn("ingest_run_id", lit(ingest_run_id))
    df_struct_good = df_struct_good.withColumn("source_file", lit(source_file))
    df_struct_good = df_struct_good.withColumn("ingest_ts", current_timestamp())
    df_struct_good = df_struct_good.withColumn("date", to_date(col("timestamp")))
    df_struct_good = df_struct_good.dropDuplicates()


    
    # 13. Business DQ rules
    
    dq_cond = (
        col("timestamp").isNull() |
        (abs(col("revenue") - (col("quantity") * col("unit_price"))) > 0.01)
    )

    df_dq_bad  = df_struct_good.filter(dq_cond)
    df_dq_good = df_struct_good.filter(~dq_cond)


    
    # 14. Align reject schemas (no mismatch)
    
    reject_columns = [
        "raw_row",
        "transaction_id",
        "store_id",
        "timestamp_raw",
        "timestamp_parsed",
        "item_id",
        "item_category",
        "quantity",
        "unit_price",
        "revenue",
        "payment_method",
        "customer_id",
        "reject_reason"
    ]

    def align_reject_schema(df):
        out = df
        for col_name in reject_columns:
            if col_name not in df.columns:
                out = out.withColumn(col_name, lit(None))
        return out.select(reject_columns)

    struct_rejects_aligned = align_reject_schema(struct_rejects)
    timestamp_invalid_aligned = align_reject_schema(timestamp_invalid)

    dq_rejects = df_dq_bad.withColumn("raw_row", lit(None)) \
                          .withColumn("reject_reason", lit("BUSINESS_LOGIC_FAIL"))

    dq_rejects_aligned = align_reject_schema(dq_rejects)

    rejects_df = (
        struct_rejects_aligned
        .unionByName(timestamp_invalid_aligned)
        .unionByName(dq_rejects_aligned)
    )

    reject_count = rejects_df.count()
    good_count   = df_dq_good.count()
    total_rows   = good_count + reject_count



    
    # 15. Write rejects
   
    if reject_count > 0:
        rejects_df.write.mode("append").json(dq_json_path)
        rejects_df.coalesce(1).write.mode("append").option("header", True).csv(dq_csv_path)


   
    # 16. SNS Summary
    
    if sns_topic:
        msg = (
            f"FILE: {source_file}\n\n"
            f"Total Rows: {total_rows}\n"
            f"Good Rows: {good_count}\n"
            f"Rejected Rows: {reject_count}\n\n"
            f"Breakdown:\n"
            f" - Missing Required Columns: {struct_rejects_aligned.count()}\n"
            f" - Invalid Timestamps: {timestamp_invalid_aligned.count()}\n"
            f" - Business Logic Rejects: {dq_rejects_aligned.count()}\n"
        )
        sns_client.publish(
            TopicArn=sns_topic,
            Subject="DATA QUALITY REPORT",
            Message=msg
        )


    
    # 17. Write GOOD rows to processed
    
    processed_path = output_path
    df_dq_good.write.mode("append").partitionBy("date").parquet(processed_path)


   
    # 18. Archive validated file — NOW with timestamp + ingest_id
    
    timestamp_now = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    archive_filename = f"{source_file}_{timestamp_now}_{ingest_run_id}"
    archive_key = f"archive/validated/{archive_filename}"

    s3.copy_object(
        Bucket=bucket_name,
        CopySource={"Bucket": bucket_name, "Key": validated_key},
        Key=archive_key
    )
    s3.delete_object(Bucket=bucket_name, Key=validated_key)


    
    # 19. Finish successfully
    
    job.commit()
    print("Job completed successfully.")




# GLOBAL FAILURE HANDLING (CATCH ANY FAILURE)

except Exception as e:

    print("CRITICAL ERROR:", str(e))
    print("Moving file to rejected/system and cleaning partially written data...")

    # CLEANUP: DELETE partially written processed partition
    try:
        date_partitions = spark.sql("SELECT DISTINCT date FROM df_struct_good").collect()
        for row in date_partitions:
            if row.date:
                bad_path = f"{output_path}/date={row.date}"
                try:
                    s3.delete_object(Bucket=bucket_name, Key=bad_path)
                except:
                    pass
    except:
        pass

    # Move file to system reject
    move_to_system_reject(str(e))

    # Fail job
    raise e
