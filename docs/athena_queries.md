# Athena Query Examples

## Create external table (if not using crawler)
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS retail_db.fact_sales (
  transaction_id string,
  store_id string,
  timestamp timestamp,
  item_id string,
  item_category string,
  quantity bigint,
  unit_price double,
  revenue double,
  payment_method string,
  customer_id string
)
PARTITIONED BY (date string)
STORED AS PARQUET
LOCATION 's3://<bucket>/gold/fact_sales/';
```

## Sample analytics queries
- Daily revenue:
```sql
SELECT date, SUM(revenue) as total_revenue
FROM retail_db.fact_sales
GROUP BY date
ORDER BY date DESC;
```

- Top products by revenue:
```sql
SELECT item_id, SUM(revenue) as revenue
FROM retail_db.fact_sales
GROUP BY item_id
ORDER BY revenue DESC
LIMIT 10;
```
