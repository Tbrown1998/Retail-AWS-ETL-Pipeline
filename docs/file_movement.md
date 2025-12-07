# File Movement & Lifecycle Rules

flowchart TD

    subgraph RAW["S3 RAW ZONE"]
        A[Incoming Retail CSV Files]
    end

    A -->|S3 Event Trigger| LAMBDA

    subgraph LAMBDA["Lambda Validator"]
        LAMBDA_HEADER[Header Normalization\nDelimiter Detection\nRequired Columns Check]
        LAMBDA_DQ[Basic Quick DQ Checks]
    end

    LAMBDA -->|Valid| VALIDATED
    LAMBDA -->|Invalid\nMove File + reason.json| REJECT_SYSTEM

    subgraph VALIDATED["S3 VALIDATED ZONE"]
        B[Validated Files Ready for Glue]
    end

    subgraph REJECT_SYSTEM["rejected/system/"]
        R1[Failed File]
        R2[<file>_reason.json]
    end
    
    B -->|Glue Trigger| GLUE1

    subgraph GLUE1["Glue ETL Job (Raw → Processed)"]
        CLEAN[Clean & Normalize\nParse Columns\nTimestamp Parsing\nNumeric Cleanup]
        DQ1[Business Rules\n(revenue = qty * price)\nMissing Required Fields]
        WRITE_PROCESSED[Write Good Rows to\nprocessed/date=YYYY-MM-DD/]
        WRITE_DQ_REJECTS[Write Rejects\nJSON + CSV]
        ARCHIVE[Archive Validated File]
        FAIL1[On Failure: Move Validated to rejected/system]
    end

    GLUE1 -->|Good Rows| PROCESSED
    GLUE1 -->|DQ Rejects| REJECT_DQ
    GLUE1 -->|Fatal Error| REJECT_SYSTEM

    subgraph PROCESSED["S3 PROCESSED ZONE"]
        P[Partitioned Parquet Files\n(date=YYYY-MM-DD)]
    end

    subgraph REJECT_DQ["rejected/data_quality/"]
        RDQ1[JSON Rejects]
        RDQ2[CSV Rejects]
    end

    P -->|Scheduled or On-Demand| GLUE2

    subgraph GLUE2["Glue GOLD Compaction Job"]
        READ_P[Read Processed Partitions]
        DEDUP[Deduplicate by transaction_id\nLatest ingest_ts]
        ROW_HASH[Compute Row Hash]
        WRITE_GOLD[Write Gold Partition]
        AUDIT[Write metrics.json]
    end

    GLUE2 --> GOLD

    subgraph GOLD["S3 GOLD ZONE\nFact Tables (Analytics Ready)"]
        G[gold/fact_sales/date=YYYY-MM-DD/]
    end

    GOLD --> CRAWLER

    subgraph CRAWLER["Glue Crawler (Optional)"]
        CAT[Update Glue Catalog\nAthena Table]
    end

    CAT --> ATHENA

    subgraph ATHENA["Athena / BI Tools"]
        Q[Analytical Queries\nDashboards]
    end


- Lambda always moves files out of `raw/` (either validated/ or rejected/system/)
- Glue always moves files out of `validated/` (on success to archive/validated/, on failure to rejected/system/)
- On successful Glue run, validated file archived as:
  `archive/validated/<original_filename>_<YYYYMMDDTHHMMSS>_<ingest_run_id>`
- GOLD compaction job reads processed partitions and writes to `gold/fact_sales/date=YYYY-MM-DD/`
- No files should remain stuck in `raw/` or `validated/` after processing
