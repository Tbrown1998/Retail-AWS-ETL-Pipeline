# End-to-End Dataflow

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
