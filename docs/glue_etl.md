# Glue ETL (Raw → Processed)

This Glue job (primary ETL) performs the heavy lifting to turn validated CSVs into partitioned Parquet datasets.

Key steps:
- Read raw text file from S3
- Remove BOM and invisible characters
- Detect delimiter robustly (csv.Sniffer + safe fallback)
- Extract header row, normalize header names, map synonyms to canonical names
- Split data rows into `cols` array using header-based mapping
- Extract required and optional fields; rows missing required fields are structural rejects
- Hardened multi-format timestamp parsing using regex gating before `to_timestamp`
- Clean numeric columns (remove currency symbols, parentheses => negatives), cast types
- Add metadata: ingest_run_id, source_file, ingest_ts, date
- Apply business DQ rules (timestamp not null, revenue ≈ quantity * unit_price)
- Separate df into df_dq_good and df_dq_bad
- Align reject schemas and write rejects to JSON + CSV
- Write df_dq_good to `processed/.../date=YYYY-MM-DD/` as parquet (partitioned)
- Archive validated file after success
- On exception: delete partial outputs, move validated file to `rejected/system/`, write reason.json, publish SNS

Important helper functions:
- `align_reject_schema(df)` ensures all reject frames have identical column layout for union
- Timestamp parsing block uses pattern/format pairs and `coalesce(when(rlike, to_timestamp(...)))`
- Delimiter detection falls back to counting candidate delimiters for older Glue runtimes
