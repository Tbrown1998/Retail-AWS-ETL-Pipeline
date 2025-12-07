# IAM Roles & Permissions

This document describes all AWS IAM roles and permissions required for the Retail AWS ETL Pipeline.  
Each role enables secure interaction between Lambda, Glue, S3, SNS, CloudWatch, and the Data Catalog.

---

## 1. Lambda Validator Role (`LambdaValidationRole`)

**Used by:** Lambda validator handling RAW → VALIDATED / REJECTED decisions.

### Required Permissions
```json
{
  "s3:GetObject": "raw/*",
  "s3:PutObject": ["validated/*", "rejected/system/*"],
  "s3:DeleteObject": "raw/*",
  "sns:Publish": "*",
  "logs:*": "*"
}
```

### Responsibilities
- Read RAW input files  
- Write VALIDATED files  
- Write SYSTEM rejects (`rejected/system/...`)  
- Remove processed files from RAW to prevent duplication  
- Publish SNS alerts on failure  
- Emit CloudWatch logs  

---

## 2. Glue ETL Job Role (`GlueETLRole`)

**Used by:** Main Glue ETL job that processes VALIDATED → PROCESSED.

### Required Permissions
```json
{
  "s3:GetObject": ["validated/*"],
  "s3:PutObject": ["processed/*", "rejected/data_quality/*", "archive/*"],
  "s3:DeleteObject": ["validated/*", "processed/*"],
  "sns:Publish": "*",
  "logs:*": "*",
  "glue:StartCrawler": "*"
}
```

### Responsibilities
- Read validated files from S3  
- Write processed parquet partitions  
- Write DQ reject files (JSON/CSV)  
- Cleanup partial output folders on failure  
- Archive validated files upon success  
- Start Glue crawler (optional)  
- Publish DQ/system SNS alerts  

---

## 3. Glue Gold Compaction Role (`GlueGoldRole`)

**Used by:** Glue job that merges and compacts PROCESSED → GOLD.

### Required Permissions
```json
{
  "s3:GetObject": ["processed/*"],
  "s3:PutObject": ["gold/*", "audit/gold_compaction/*"],
  "s3:DeleteObject": ["gold/*"],
  "logs:*": "*",
  "glue:StartCrawler": "*"
}
```

### Responsibilities
- Detect and read processed partitions  
- Deduplicate records  
- Write curated FACT table into the gold zone  
- Overwrite partitions safely  
- Write audit metrics JSON files  
- Trigger Glue crawler (optional)  
- Emit logs to CloudWatch  

---

## 4. Glue Crawler Role (`GlueCrawlerRole`)

**Used by:** Crawler responsible for schema discovery in PROCESSED and GOLD zones.

### Required Permissions
```json
{
  "s3:GetObject": ["processed/*", "gold/*"],
  "glue:*": "*",
  "logs:*": "*"
}
```

### Responsibilities
- Scan S3 parquet folders  
- Detect schema and update Glue Data Catalog  
- Maintain table partitions  
- Log events to CloudWatch  

---

## 5. SNS Notification Permissions

**Used by:** Lambda Validator and Glue ETL Job.

### Required Permissions
```json
{
  "sns:Publish": "arn:aws:sns:*"
}
```

### Responsibilities
- Send system-failure alerts  
- Send DQ rejection notifications  
- Send pipeline-level warnings  

---

## 6. CloudWatch Logging Permissions

**Used by:** Lambda, Glue ETL, Glue Gold, Crawlers.

### Required Permissions
```json
{
  "logs:CreateLogGroup": "*",
  "logs:CreateLogStream": "*",
  "logs:PutLogEvents": "*"
}
```

### Responsibilities
- Write operational logs  
- Allow debugging and metric collection  

---

## 7. Optional S3 Bucket Policy (Recommended for Security)

### Recommended Controls
- Deny unencrypted S3 uploads  
- Deny non-TLS S3 access  
- Block all public access  
- Restrict write access to pipeline IAM roles  
- Require server-side encryption  

---

## 8. Summary Table

| Component         | IAM Role             | Purpose                                          |
|------------------|----------------------|--------------------------------------------------|
| Lambda Validator | LambdaValidationRole | Validate files and move RAW → VALIDATED/REJECTED |
| Glue ETL Job     | GlueETLRole          | Transform validated data into processed parquet  |
| Glue Gold Job    | GlueGoldRole         | Compact processed partitions into gold fact table |
| Glue Crawler     | GlueCrawlerRole      | Update Data Catalog tables and partitions        |
| SNS              | —                    | Receives alerts                                  |
| CloudWatch       | —                    | Collects logs & metrics                          |

---

## 9. Diagram (Conceptual)

```
RAW → LambdaValidationRole → VALIDATED
VALIDATED → GlueETLRole → PROCESSED
PROCESSED → GlueGoldRole → GOLD
GOLD → GlueCrawlerRole → Data Catalog → Athena
SNS & CloudWatch ← All compute services
```
