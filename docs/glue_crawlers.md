# Glue Crawlers & Glue Data Catalog

## Purpose
Glue Crawlers scan S3 prefixes and populate the Glue Data Catalog with table metadata that Athena can query.

## Recommended setup
- Create a crawler targeting `processed/<table>/` and another for `gold/fact_sales/`
- Configure crawler to run on schedule or triggered by workflow (e.g., start crawler after compaction)
- Choose an appropriate Glue database to house tables (e.g., `retail_db`)

## Tips
- Enable partition projection or rely on crawler partition discovery
- For frequent partitions, consider partition projection to reduce crawler overhead
- After compaction, start crawler with `glue_client.start_crawler(Name=crawler_name)` (job supports optional `--crawler_name`)
