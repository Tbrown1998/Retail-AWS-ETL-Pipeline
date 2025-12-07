# S3 Layout (Data Lake Zones)

- `raw/` - incoming raw files
- `validated/` - files that passed Lambda validation
- `processed/` - Parquet outputs from primary Glue job (partitioned by date)
- `gold/` - compacted, deduplicated analytics-ready fact tables (partitioned by date)
  - e.g. `gold/fact_sales/date=YYYY-MM-DD/`
- `rejected/`
  - `rejected/data_quality/json/`
  - `rejected/data_quality/csv/`
  - `rejected/system/`
- `archive/validated/` - archived original files after successful processing
- `audit/` - job-level and partition-level audit metrics (gold_compaction/)
