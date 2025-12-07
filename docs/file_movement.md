# File Movement & Lifecycle Rules

- Lambda always moves files out of `raw/` (either validated/ or rejected/system/)
- Glue always moves files out of `validated/` (on success to archive/validated/, on failure to rejected/system/)
- On successful Glue run, validated file archived as:
  `archive/validated/<original_filename>_<YYYYMMDDTHHMMSS>_<ingest_run_id>`
- GOLD compaction job reads processed partitions and writes to `gold/fact_sales/date=YYYY-MM-DD/`
- No files should remain stuck in `raw/` or `validated/` after processing
