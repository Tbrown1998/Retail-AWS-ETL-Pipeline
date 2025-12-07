# Scripts & Where to put code

Place production scripts here.

- `scripts/glue_job_raw_to_processed.py` -> main Glue ETL (raw/validated -> processed/)
- `scripts/incremental_auto_compaction.py` -> gold compaction job (processed -> gold/)
- `scripts/lambda_validator.py` -> Lambda validator script

Ensure scripts are uploaded to S3 and referenced in Glue job definitions or Lambda deployments.
