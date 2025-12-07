# Reject Handling & Schema

Reject types:
- Structural: MISSING_REQUIRED_COLUMN
- Timestamp: INVALID_TIMESTAMP_FORMAT
- Business logic: BUSINESS_LOGIC_FAIL
- System: GLUE or runtime failures (moved entire file to rejected/system/)

Reject schema (each row):
- raw_row
- transaction_id
- store_id
- timestamp_raw
- timestamp_parsed
- item_id
- item_category
- quantity
- unit_price
- revenue
- payment_method
- customer_id
- reject_reason

System rejects include a reason JSON file next to the moved file:
`rejected/system/<filename>_reason.json`
