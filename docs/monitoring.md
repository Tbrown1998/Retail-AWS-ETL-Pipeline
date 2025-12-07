# Monitoring & Observability

## SNS Topics
- Data quality alerts: publishes counts and breakdowns
- System failures: publishes stacktrace excerpt and S3 locations of moved files

## CloudWatch
- Glue job logs capture full stacktraces and metrics
- Lambda logs capture header validation and routing decisions
- CloudWatch Alarms on Glue job failures recommended

## Audit metrics
- Gold compaction job writes per-partition metrics to `audit/gold_compaction/date=YYYY-MM-DD/metrics.json`
- A run-level summary is written to `audit/gold_compaction/last_run_summary.json`
