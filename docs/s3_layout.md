# S3 Layout (Data Lake Zones)

```
s3://retail-sales-demo-s3/
│
├── raw/                               # Incoming unprocessed source files
│    └── <source_file.csv>
│
├── validated/                         # Files that passed Lambda validation
│    └── <source_file.csv>
│
├── rejected/
│    ├── system/                       # Complete-system failures (lambda or glue)
│    │     ├── <failed_file.csv>                   # Moved here on fatal error
│    │     └── <failed_file>_reason.json           # Includes stacktrace / failure details
│    │
│    └── data_quality/
│          ├── json/                   # Machine-readable row-level rejects
│          │     └── part-0000.json
│          │
│          └── csv/                    # Analyst-readable rejects (coalesced = 1)
│                └── part-0000.csv
│
├── processed/                         # Silver layer (clean, validated parquet)
│    └── date=YYYY-MM-DD/              # Partitioned by event date
│          ├── part-0000.snappy.parquet
│          ├── part-0001.snappy.parquet
│          └── ...
│
├── gold/                              # Gold layer (analytics-ready fact tables)
│    └── fact_sales/
│          └── date=YYYY-MM-DD/        # Partitioned by date, deduped & compacted
│                ├── part-0000.snappy.parquet
│                └── ...
│
├── audit/
│    └── gold_compaction/
│          ├── date=YYYY-MM-DD/
│          │      └── metrics.json     # Partition-level audit metrics
│          │
│          └── last_run_summary.json   # Job-level summary file
│
├── archive/
│    └── validated/
│          └── <original_filename>_<YYYYMMDDTHHMMSS>_<ingest_run_id>
│
└── logs/ (optional)
     └── glue/
```

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
