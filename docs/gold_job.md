# GOLD Compaction Job (Processed → Gold)

This document describes the `incremental_auto_compaction.py` job that compacts daily partitions from the processed layer to the gold/fact_sales layer.

## Purpose
- Produce analytics-ready, deduplicated daily fact partitions
- Reduce number of small files through controlled coalesce
- Ensure idempotency: re-running same date overwrites gold partition
- Produce audit metrics per partition for observability and SLA reporting
- Optionally trigger Glue crawler to update catalog

## Inputs / Outputs
- Input: `processed/.../date=YYYY-MM-DD/` (Parquet)
- Output: `gold/fact_sales/date=YYYY-MM-DD/` (Parquet, overwrite)
- Audit: `audit/gold_compaction/date=YYYY-MM-DD/metrics.json`

## High-level algorithm
1. Discover processed partitions (list `date=` prefixes under processed_path)
2. Optionally filter by `--force_dates` or compute set difference vs gold partitions
3. For each chosen date:
   - Read partition with `spark.read.parquet(...)` (mergeSchema=true)
   - Ensure expected columns exist (add null defaults)
   - Defensive numeric normalization
   - Compute `row_hash` across key columns (md5 of concatenated columns)
   - Deduplicate by `transaction_id` keeping latest by `ingest_ts` or compaction time
   - Compute metrics: total_rows, rows_after_dedup, null_timestamp, dq_balance_issues, etc.
   - Write compacted partition to gold_path/date=YYYY-MM-DD/ (overwrite)
   - Write per-partition metrics JSON to audit_path
4. Emit run-level summary to audit_path/gold_compaction/last_run_summary.json
5. Optionally start a Glue crawler to update the Data Catalog

## Idempotency & Safety
- Overwrite semantics per partition ensure re-running is safe
- Job respects `--max_partitions` to limit throughput
- Handles missing partitions gracefully and records warnings in audit

## Notes on extension
- Add dimension joins (e.g., enrich store metadata) before writing to gold for denormalized facts
- Implement watermarking to handle late-arriving records
- Add SCD handling for slowly changing dimensions if needed
