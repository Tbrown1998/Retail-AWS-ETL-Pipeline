# End-to-End Dataflow

                   +------------------------+
                   |      S3 RAW ZONE       |
                   |   raw/<file>.csv       |
                   +-----------+------------+
                               |
                               | S3 Event Trigger
                               v
                   +------------------------+
                   |    Lambda Validator    |
                   | - header validation    |
                   | - delimiter detection  |
                   | - required columns     |
                   +------+-------+---------+
                          |       |
                          |       |
                     Valid|   Invalid (system reject)
                          |       |
                          v       v
        +------------------------+      +------------------------------+
        |     S3 VALIDATED       |      |    rejected/system/         |
        | validated/<file>.csv   |      | file + reason.json          |
        +-----------+------------+      +------------------------------+
                    |
                    | Trigger Glue
                    v
        +--------------------------------------------------------------+
        |     Glue Job #1: Raw → Processed                             |
        |  - Clean & split rows                                        |
        |  - Header-based mapping                                      |
        |  - Timestamp parsing                                         |
        |  - Numeric normalization                                     |
        |  - Business DQ rules                                         |
        |  - Write good rows to processed/date=YYYY-MM-DD/             |
        |  - Write rejects to rejected/data_quality/                   |
        |  - Archive validated file                                    |
        |  - On failure: move validated to rejected/system/            |
        +-------------+-----------------+-------------------------------+
                      |                 |
               Good Rows        DQ Rejects / Structural Rejects
                      |                 |
                      v                 v
    +-------------------------+    +------------------------------+
    |     S3 PROCESSED       |    |  rejected/data_quality/      |
    | processed/date=YYYY-MM-DD/   |  json + csv                 |
    +-------------+---------------+------------------------------+
                  |
                  | Scheduled / On Demand
                  v
        +--------------------------------------------------------------+
        |     Glue Job #2: GOLD Compaction (Processed → Gold)          |
        |  - Deduplicate by transaction_id                             |
        |  - Compute row_hash                                          |
        |  - Coalesce files                                            |
        |  - Write to gold/fact_sales/date=YYYY-MM-DD/                 |
        |  - Write audit metrics                                       |
        +-------------+------------------------------------------------+
                      |
                      v
            +-----------------------+
            |      GOLD ZONE       |
            | gold/fact_sales/     |
            +-----------------------+
                      |
                      v
         +-----------------------------+
         |   Glue Crawler (Optional)   |
         +--------------+--------------+
                        |
                        v
         +-----------------------------+
         | Athena / BI Visualization   |
         +-----------------------------+


1. Producer uploads CSV to `raw/` (S3 PUT)
2. S3 Event triggers Lambda: header & schema checks, delimiter detection, quick DQ
   - If pass: move file to `validated/`
   - If fail: move file to `rejected/system/` with reason JSON & CSV
3. Glue ETL job (raw->processed):
   - reads files from `validated/`
   - BOM / whitespace cleaning
   - delimiter detection
   - header normalization & mapping
   - parse rows using header-based mapping
   - hardened timestamp parsing
   - numeric normalization
   - apply business DQ rules
   - write good rows to `processed/.../date=YYYY-MM-DD/` partitioned parquet
   - write rejects to `rejected/data_quality/json/` and `rejected/data_quality/csv/`
   - on success: archive validated file to `archive/validated/<filename>_<ts>_<ingest_run_id>`
   - on any failure: delete partial outputs, move validated file to `rejected/system/`, write reason.json, send SNS
4. GOLD compaction job:
   - periodic or triggered job reads partitions in `processed/.../date=YYYY-MM-DD/`
   - deduplicates rows (keeps latest by ingest_ts)
   - computes row_hash for change detection
   - writes partition to `gold/fact_sales/date=YYYY-MM-DD/` (overwrite)
   - writes audit metrics to `audit/gold_compaction/date=YYYY-MM-DD/metrics.json`
   - optionally triggers Glue crawler to update Data Catalog
5. Downstream: analysts query gold via Athena or BI tools; further ETL to marts possible
