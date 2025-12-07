import os
import json
import logging
import boto3
import csv
import uuid
from datetime import datetime
from urllib.parse import unquote_plus
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sns = boto3.client("sns")
glue = boto3.client("glue")

# Environment variables
BUCKET = os.environ.get("BUCKET")  
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/")
VALIDATED_PREFIX = os.environ.get("VALIDATED_PREFIX", "validated/")
STRUCTURAL_REJECT_PREFIX = os.environ.get("STRUCTURAL_REJECT_PREFIX", "rejected/structural/")
SYSTEM_REJECT_PREFIX = os.environ.get("SYSTEM_REJECT_PREFIX", "rejected/system/")
ARCHIVE_PREFIX = os.environ.get("ARCHIVE_PREFIX", "archive/")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME")
MAX_BYTES_TO_READ = int(os.environ.get("MAX_BYTES_TO_READ", "65536"))

REQ_COLS_ENV = os.environ.get("REQUIRED_COLUMNS")
if REQ_COLS_ENV:
    REQUIRED_COLUMNS = [c.strip() for c in REQ_COLS_ENV.split(",") if c.strip()]
else:
    REQUIRED_COLUMNS = [
        "transaction_id", "store_id", "timestamp", "item_id", "item_category",
        "quantity", "unit_price", "revenue", "payment_method", "customer_id"
    ]

PREFERRED_DELIMITERS = [",", ";", "\t", "|"]



# Helper functions

def now_ts():
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def gen_uuid():
    return uuid.uuid4().hex[:8]


def basename(key):
    return key.split("/")[-1]


def extension(name):
    idx = name.rfind(".")
    return name[idx:] if idx != -1 else ""


def name_with_option_c(orig_name, tag):
    base = orig_name
    ext = extension(base)
    base_no_ext = base[:len(base) - len(ext)] if ext else base
    return f"{base_no_ext}__{tag}__{now_ts()}__{gen_uuid()}{ext}"


def read_object_head(bucket, key, num_bytes=MAX_BYTES_TO_READ):
    try:
        resp = s3.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{num_bytes-1}")
        return resp["Body"].read()
    except ClientError:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()


def detect_delimiter_and_header(sample_bytes):
    text = sample_bytes.decode("utf-8", errors="replace")
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(text, delimiters=PREFERRED_DELIMITERS)
        delim = dialect.delimiter
        first_line = next((ln for ln in text.splitlines() if ln.strip()), "")
        header = next(csv.reader([first_line], delimiter=delim))
        return delim, [h.strip() for h in header]
    except Exception:
        lines = [ln for ln in text.splitlines() if ln.strip()][:5]
        if not lines:
            return None, []
        best_delim, best_len, best_header = None, -1, []
        for d in PREFERRED_DELIMITERS:
            parts = lines[0].split(d)
            if len(parts) > best_len:
                best_delim, best_len, best_header = d, len(parts), parts
        return best_delim, [h.strip() for h in best_header]


def move_s3_object(bucket, source_key, target_key):
    logger.info("MOVING %s → %s", source_key, target_key)
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": source_key}, Key=target_key)
    s3.delete_object(Bucket=bucket, Key=source_key)


def write_reason_json(bucket, key, payload):
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(payload).encode("utf-8"))


def send_alert(subject, message):
    if not SNS_TOPIC_ARN:
        return
    sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)



# Lambda handler

def lambda_handler(event, context):
    logger.info("Event: %s", json.dumps(event))

    try:
        for record in event.get("Records", []):
            s3_info = record.get("s3", {})
            bucket = s3_info.get("bucket", {}).get("name")
            key = unquote_plus(s3_info.get("object", {}).get("key"))

            if bucket != BUCKET or not key.startswith(RAW_PREFIX):
                continue

            orig_name = basename(key)
            ingest_run_id = gen_uuid()

            validated_name = name_with_option_c(orig_name, "validated")
            structural_name = name_with_option_c(orig_name, "structural")
            archive_raw_name = name_with_option_c(orig_name, "archived_raw")

            archive_raw_key = f"{ARCHIVE_PREFIX}raw/{archive_raw_name}"
            move_s3_object(bucket, key, archive_raw_key)

            sample = read_object_head(bucket, archive_raw_key)
            if not sample:
                dst = f"{SYSTEM_REJECT_PREFIX}{structural_name}"
                move_s3_object(bucket, archive_raw_key, dst)
                write_reason_json(bucket, dst + "_reason.json", {"file": archive_raw_key})
                send_alert("SYSTEM ERROR", archive_raw_key)
                continue

            delimiter, header = detect_delimiter_and_header(sample)

            structural_errors = []
            if delimiter is None:
                structural_errors.append("delimiter_detection_failed")
            missing = [c for c in REQUIRED_COLUMNS if c not in header]
            if missing:
                structural_errors.append(f"missing_columns:{missing}")

            if structural_errors:
                dst = f"{STRUCTURAL_REJECT_PREFIX}{structural_name}"
                move_s3_object(bucket, archive_raw_key, dst)
                write_reason_json(bucket, dst + "_reason.json", {"errors": structural_errors})
                send_alert("STRUCTURAL REJECT", json.dumps(structural_errors))
                continue

            validated_key = f"{VALIDATED_PREFIX}{validated_name}"
            move_s3_object(bucket, archive_raw_key, validated_key)

            # FIXED HERE → pass validated_key to Glue, NOT raw key
            glue_args = {
                "--s3_input_path": f"s3://{bucket}/{validated_key}",
                "--s3_output_path": f"s3://{bucket}/processed/",
                "--ingest_run_id": ingest_run_id,
                "--source_file": validated_name,
                "--original_key": validated_key,          
                "--sns_topic_arn": SNS_TOPIC_ARN          
            }

            try:
                glue.start_job_run(JobName=GLUE_JOB_NAME, Arguments=glue_args)
            except Exception as e:
                sys_key = f"{SYSTEM_REJECT_PREFIX}{validated_name}"
                move_s3_object(bucket, validated_key, sys_key)
                write_reason_json(bucket, sys_key + "_reason.json", {"error": str(e)})
                send_alert("GLUE START FAILURE", str(e))

    except Exception as exc:
        send_alert("LAMBDA FATAL ERROR", str(exc))
        raise

    return {"status": "ok"}
