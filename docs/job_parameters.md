# 🧩 Job Parameters Summary

This section summarizes all required and optional job parameters used across the pipeline’s Lambda validator, Glue ETL job, and Glue GOLD compaction job.

---

# 1️⃣ Lambda Validator — Environment Variables

| Parameter | Required | Description |
|----------|----------|-------------|
| **BUCKET_NAME** | ✔️ | Target S3 bucket for all layers. |
| **VALIDATED_PREFIX** | ✔️ | Prefix where successfully validated files are moved (`validated/`). |
| **REJECT_SYSTEM_PREFIX** | ✔️ | Prefix for system-level rejects (`rejected/system/`). |
| **SNS_TOPIC_ARN** | Optional | SNS topic for validation failure notifications. |
| **REQUIRED_COLUMNS** | ✔️ | Required canonical columns after header normalization. |
| **HEADER_SYNONYMS** | ✔️ | Mapping of header variations → canonical column names. |

Lambda Output:
- Valid → `validated/`
- Invalid → `rejected/system/` + `<filename>_reason.json`

---

# 2️⃣ Glue ETL Job (Raw → Processed)

### Required Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| **--JOB_NAME** | ✔️ | Glue job name. |
| **--s3_input_path** | ✔️ | Validated file path (e.g., `s3://bucket/validated/file.csv`). |
| **--s3_output_path** | ✔️ | Output prefix for processed parquet (`processed/`). |
| **--ingest_run_id** | ✔️ | Unique ID applied to all processed rows. |
| **--source_file** | ✔️ | Original file name. |
| **--original_key** | ✔️ | Original location of file in `raw/`. |
| **--sns_topic_arn** | Optional | SNS topic for DQ/system failure alerts. |

### Behavior Controlled by Params

| Parameter | Function |
|-----------|----------|
| `s3_input_path` | Identifies which validated file to process. |
| `s3_output_path` | Determines where to write processed partitions. |
| `ingest_run_id` | Tracks lineage in processed + gold layers. |
| `source_file` | Carried through the pipeline for traceability. |
| `original_key` | Logged in rejection/system files. |
| `sns_topic_arn` | Sends DQ and system-level notifications. |

---

# 3️⃣ Glue GOLD Compaction Job (Processed → Gold)

Script: `incremental_auto_compaction.py`

### Required Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| **--JOB_NAME** | ✔️ | Glue job name. |
| **--processed_path** | ✔️ | Input prefix (`processed/`). |
| **--gold_path** | ✔️ | Output prefix (`gold/fact_sales/`). |
| **--audit_path** | ✔️ | S3 location for metrics. |

### Optional Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| **--max_partitions** | Optional | Max partitions to process per run (default `10`). |
| **--reprocess** | Optional | Reprocess partitions even if gold version exists (`true/false`). |
| **--force_dates** | Optional | Comma-separated list of dates to process (`YYYY-MM-DD`). |
| **--crawler_name** | Optional | Glue Crawler to start after compaction. |

### Parameter Behavior

| Parameter | Purpose |
|----------|----------|
| `processed_path` | Reads input partitions (`date=YYYY-MM-DD/`). |
| `gold_path` | Writes curated gold table. |
| `audit_path` | Writes per-partition and job-level metrics. |
| `max_partitions` | Helps tune compute cost. |
| `reprocess` | Forces override of existing gold partitions. |
| `force_dates` | Direct control over which partitions to run. |
| `crawler_name` | Updates Glue Data Catalog after write. |

---

# 4️⃣ Parameter Map Across Entire Pipeline

| Parameter | Used In | Required | Description |
|-----------|---------|----------|-------------|
| `BUCKET_NAME` | Lambda | ✔️ | Root S3 bucket used by pipeline. |
| `VALIDATED_PREFIX` | Lambda | ✔️ | Destination for valid files. |
| `REJECT_SYSTEM_PREFIX` | Lambda | ✔️ | Destination for invalid/system-failed files. |
| `SNS_TOPIC_ARN` | Lambda / Glue | Optional | Sends notifications. |
| `JOB_NAME` | Glue | ✔️ | Job name for Glue and logs. |
| `s3_input_path` | Glue ETL | ✔️ | Input validated file path. |
| `s3_output_path` | Glue ETL | ✔️ | Output prefix for processed data. |
| `ingest_run_id` | Glue ETL | ✔️ | Metadata used throughout pipeline. |
| `source_file` | Glue ETL | ✔️ | Original filename. |
| `original_key` | Glue ETL | ✔️ | Original raw file path. |
| `processed_path` | Gold Job | ✔️ | Input dataset for compaction. |
| `gold_path` | Gold Job | ✔️ | Output fact table prefix. |
| `audit_path` | Gold Job | ✔️ | Output metrics folder. |
| `max_partitions` | Gold Job | Optional | Partition count limit. |
| `reprocess` | Gold Job | Optional | Whether to overwrite existing gold data. |
| `force_dates` | Gold Job | Optional | Manual override of dates to process. |
| `crawler_name` | Gold Job | Optional | Glue crawler name to run after job. |

---

# ✔️ Summary

This table covers **every parameter this pipeline depends on**, including:
- Ingestion-time validation (Lambda)
- Raw → Processed ETL (Glue Job 1)
- Processed → Gold compaction (Glue Job 2)
- Monitoring, notifications, and file routing

--- 
