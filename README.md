# Retail AWS ETL Pipeline
A Production-Grade, Serverless Data Ingestion & Transformation Framework for Retail Transaction Files


![alt text](<imgs/Data flow image.png>)

This repository contains a fully automated, fault-tolerant, and highly extensible **retail data ingestion pipeline** built on **AWS serverless technologies** (S3, Lambda, Glue, SNS). It handles semi-structured CSV retail transaction files, validates them, enforces business rules, transforms them using PySpark, and stores clean, query-ready datasets in a data lake architecture.

The pipeline enforces industry best practices:
- event-driven ingestion
- multi-layer S3 data lake organization (raw, validated, processed, gold, rejected, archive)
- header-flexible parsing with synonym mapping
- hardened multi-format timestamp parsing
- business data-quality enforcement
- atomic processing (no partial partitions)
- detailed rejection logging (JSON + CSV)
- observability via SNS + CloudWatch
- archival with traceable naming

## Folder structure (Repository)
```
RETAIL-AWS-ETL-PIPELINE/
│
├── .git/
│
├── README.md
│
├── docs/
│   ├── architecture.md
│   ├── athena_queries.md
│   ├── dataflow.md
│   ├── file_movement.md
│   ├── glue_crawlers.md
│   ├── glue_etl.md
│   ├── gold_job.md
|   ├── iam_roles_permissions.md
│   ├── job_parameters.md
│   ├── lambda_validation.md
│   ├── monitoring.md
│   ├── rejects.md
│   ├── s3_layout.md
│   ├── schema_mapping.md
│   ├── scripts.md
│   ├── timestamp_parsing.md
│   ├── troubleshooting.md
│   ├── validation.md
│
├── imgs/
│   ├── (architecture and data flow images)
│
├── sample_csv_files/
│   ├── sales_2024-10-16.csv
│   ├── sales_2024-12-07.csv
│   ├── sales_2025-06-12.csv
│   ├── sales_2025-09-03.csv
│   ├── sales_2025-10-18.csv
│
└── scripts/
    ├── glue_job_raw_to_processed.py
    ├── incremental_auto_compaction.py
    └── lambda_validator.py

```

## Key Capabilities
- Flexible header-based mapping (synonyms & variations supported)
- Automatic delimiter detection (, ; | \t)
- Hardened multi-format timestamp parsing (regex-gated)
- Strict data quality with structured + human-readable reject logs
- Zero partial ingestion — atomic writes with rollback
- Partitioned Parquet output optimized for Athena / Glue Catalog
- GOLD layer (analytics-ready) compaction and deduplication job
- SNS-based notifications for summary and system failures
- Archival naming: `<filename>_<YYYYMMDDTHHMMSS>_<ingest_run_id>`

## High-Level Architecture
```
raw/  →  Lambda Validator
         → validated/ (pass)
         → rejected/system/ (fail)

validated/ → Glue ETL (bronze→silver processed/)
             → processed/ (parquet partitioned by date)
             → rejected/data_quality/ (row-level rejects)
             → rejected/system/ (glue errors)
             → archive/validated/<filename>_<ts>_<ingest_run_id>

processed/ → GOLD compaction (Glue job 2)
             → gold/fact_sales/date=YYYY-MM-DD/
             → optional Glue crawler -> Glue Catalog / Athena
```


## Gold layer summary
The GOLD layer contains curated, analytics-ready tables (facts) created from processed data. A separate Glue job (`incremental_auto_compaction.py`) compacts daily partitions from `processed/` into `gold/fact_sales/` performing deduplication, basic data normalization, row hashing, and audit metrics. The gold job writes per-partition audit JSON files and optionally triggers a Glue crawler to update the Glue Data Catalog.

## How to use this repo
1. Review `docs/` for detailed architecture, flow, and operational steps.
2. Place production-ready scripts in `scripts/` (Glue jobs and Lambda).
3. Deploy Lambda to validate files dropped to `raw/`.
4. Deploy Glue job for raw->processed (bronze/silver).
5. Deploy the gold compaction Glue job to run periodically or on-demand.
6. Configure Glue crawlers and Athena for querying the processed and gold layers.

---

## 🔐 IAM Roles Overview

This pipeline uses dedicated IAM roles to ensure secure, least-privilege access across all AWS services involved.

### **LambdaValidationRole**
Handles RAW → VALIDATED/REJECTED:
- Read from `raw/`
- Write to `validated/` and `rejected/system/`
- Delete processed RAW files
- Publish SNS alerts
- Write CloudWatch logs

### **GlueETLRole**
Used by the Glue ETL job (VALIDATED → PROCESSED):
- Read from `validated/`
- Write to `processed/`, `rejected/data_quality/`, and `archive/`
- Delete validated files after success
- Publish SNS notifications  
- Optionally start Glue crawlers

### **GlueGoldRole**
Used by the Gold Compaction job (PROCESSED → GOLD):
- Read processed partitions
- Write compacted gold data and audit metrics
- Overwrite existing partitions safely
- Start Glue crawler (optional)
- Write logs to CloudWatch

### **GlueCrawlerRole**
Used by Glue Crawlers:
- Read `processed/` and `gold/` folders
- Update Glue Data Catalog tables & partitions
- Emit logs to CloudWatch

### **Monitoring Roles**
All compute services (Lambda + Glue) have:
- CloudWatch logging permissions  
- SNS publish permissions for alerts

### **Bucket Policy (Recommended)**
- Block public access  
- Enforce SSL  
- Enforce encryption  
- Allow only pipeline IAM roles to write  

These roles together enforce a secure, production-grade, least-privilege architecture where every service can interact safely while keeping data protected and traceable.

```
flowchart LR

    %% STYLE
    classDef role fill=#f9f9f9,stroke=#555,stroke-width=1px,color=#000,border-radius=6px;
    classDef service fill=#eef7ff,stroke=#4a90e2,stroke-width=1px,border-radius=6px;
    classDef bucket fill=#fef7e0,stroke=#e2a93b,stroke-width=1px,border-radius=6px;
    classDef monitor fill=#fdeaea,stroke=#e26a6a,stroke-width=1px,border-radius=6px;

    %% SERVICES
    RAW((S3 RAW)):::bucket
    VALIDATED((S3 VALIDATED)):::bucket
    PROCESSED((S3 PROCESSED)):::bucket
    GOLD((S3 GOLD)):::bucket

    SNS((SNS Topics)):::monitor
    CW((CloudWatch Logs)):::monitor
    CATALOG((Glue Data Catalog)):::service

    %% ROLES
    LAMBDA_ROLE([LambdaValidationRole]):::role
    GLUE_ETL_ROLE([GlueETLRole]):::role
    GLUE_GOLD_ROLE([GlueGoldRole]):::role
    CRAWLER_ROLE([GlueCrawlerRole]):::role

    %% LAMBDA VALIDATOR
    RAW -->|read| LAMBDA_ROLE
    LAMBDA_ROLE -->|write| VALIDATED
    LAMBDA_ROLE -->|write rejects| RAW
    LAMBDA_ROLE -->|publish| SNS
    LAMBDA_ROLE -->|logs| CW

    %% GLUE ETL ROLE
    VALIDATED -->|read| GLUE_ETL_ROLE
    GLUE_ETL_ROLE -->|write processed| PROCESSED
    GLUE_ETL_ROLE -->|write rejects| RAW
    GLUE_ETL_ROLE -->|archive validated| VALIDATED
    GLUE_ETL_ROLE -->|publish| SNS
    GLUE_ETL_ROLE -->|logs| CW

    %% GOLD ROLE
    PROCESSED -->|read| GLUE_GOLD_ROLE
    GLUE_GOLD_ROLE -->|write gold| GOLD
    GLUE_GOLD_ROLE -->|audit metrics| RAW
    GLUE_GOLD_ROLE -->|start crawler| CRAWLER_ROLE
    GLUE_GOLD_ROLE -->|logs| CW

    %% CRAWLER ROLE
    GOLD -->|read| CRAWLER_ROLE
    PROCESSED -->|read| CRAWLER_ROLE
    CRAWLER_ROLE -->|update tables| CATALOG
    CRAWLER_ROLE -->|logs| CW


```

---

# 📫 Contact

## Oluwatosin Amosu Bolaji 
- Data Engineer 
- Buiness Intelligence Analyst
- ETL Developer

#### 🚀 **Always learning. Always building. Data-driven to the core.**  

### 📫 **Let’s connect!**  
- 📩 oluwabolaji60@gmail.com
- 🔗 : [LinkedIn](https://www.linkedin.com/in/oluwatosin-amosu-722b88141)
- 🌐 : [My Portfolio](https://www.datascienceportfol.io/oluwabolaji60) 
- 𝕏 : [Twitter/X](https://x.com/thee_oluwatosin?s=21&t=EqoeQVdQd038wlSUzAtQzw)
- 🔗 : [Medium](https://medium.com/@oluwabolaji60)
- 🔗 : [View my Repositories](https://github.com/Tbrown1998?tab=repositories)
