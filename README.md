# Retail AWS ETL Pipeline
A Production-Grade, Serverless Data Ingestion & Transformation Framework for Retail Transaction Files

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
retail-aws-etl-pipeline/
├── README.md
├── docs/
│   ├── architecture.md
│   ├── dataflow.md
│   ├── lambda_validation.md
│   ├── glue_etl.md
│   ├── gold_job.md
│   ├── schema_mapping.md
│   ├── timestamp_parsing.md
│   ├── validation.md
│   ├── rejects.md
│   ├── file_movement.md
│   ├── s3_layout.md
│   ├── monitoring.md
│   ├── troubleshooting.md
│   ├── glue_crawlers.md
│   ├── athena_queries.md
│   └── scripts.md
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
