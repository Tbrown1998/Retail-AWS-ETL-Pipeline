# Troubleshooting Guide

Common issues:
- `could not be parsed at index X`: timestamp parsing partial-match; ensure regex gating before to_timestamp
- `UNRESOLVED_COLUMN: cols`: created when cols array is referenced before split_df exists; ensure split_df = data_df.withColumn('cols', split(...))
- `NUM_COLUMNS_MISMATCH` on union: ensure reject DataFrames are aligned to same column list before union
- `max() got unexpected keyword 'key'`: use sorted(dict.items(), key=...) fallback in older Glue runtimes
- `Column is not iterable`: use Spark SQL functions rather than Python iteration on Column objects

Operational tips:
- Test Glue jobs using Glue development endpoint or local PySpark
- Limit partitions via `--max_partitions` to control compaction load
- Use Glue crawler after gold compaction to update Data Catalog for Athena
