# Header & Schema Mapping

Normalization rules:
- Lowercase header strings
- Replace spaces and dashes with underscores
- Remove non-alphanumeric characters except underscore
- Apply synonym mapping to map variants to canonical column names

Example synonyms:
```
transactionid -> transaction_id
txn_id -> transaction_id
storeid -> store_id
qty -> quantity
unitprice -> unit_price
price -> unit_price
amount -> revenue
```

Extraction:
- Build index_map from normalized headers
- For each canonical column, if missing in header -> treat as null and mark structural rejects
- Only required columns cause structural rejection; extra columns are ignored
