# Glue Crawlers & Glue Data Catalog
flowchart TD
    S3P[Processed Layer\ns3://bucket/processed/date=YYYY-MM-DD/] --> CRAWL_P

    subgraph CRAWL_P["Glue Crawler: processed_crawler"]
        CP1[Scan Parquet Schema\n+ Discover Partitions]
        CP2[Update Glue Data Catalog:\nprocessed_fact_sales table]
        CP3[Make data queryable in Athena]
    end

    CRAWL_P --> ATHENA_P[Athena Queries\n(Processed Data)]

    S3G[Gold Layer\ns3://bucket/gold/fact_sales/date=YYYY-MM-DD/] --> CRAWL_G

    subgraph CRAWL_G["Glue Crawler: gold_crawler"]
        CG1[Scan Curated Gold Schema]
        CG2[Update Glue Catalog:\nfact_sales_gold table]
        CG3[Optimize Partition Projection (Optional)]
    end

    CRAWL_G --> ATHENA_G[Athena Queries\n(Gold Analytics)]

    GLUE_GOLD[Glue Gold Job\n(Compaction)] -->|Optionally triggers| CRAWL_G


## Purpose
Glue Crawlers scan S3 prefixes and populate the Glue Data Catalog with table metadata that Athena can query.

## Recommended setup
- Create a crawler targeting `processed/<table>/`, `rejected/<table>/`and another for `gold/fact_sales/`
- Configure crawler to run on schedule or triggered by workflow (e.g., start crawler after compaction)
- Choose an appropriate Glue database to house tables (e.g., `retail_db`)

## Tips
- Enable partition projection or rely on crawler partition discovery
- For frequent partitions, consider partition projection to reduce crawler overhead
- After compaction, start crawler with `glue_client.start_crawler(Name=crawler_name)` (job supports optional `--crawler_name`)
