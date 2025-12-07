# Lambda Validation

Lambda responsibilities:
- Read first non-empty line as header
- Normalize header (lowercase, spaces/dashes -> underscores, strip special characters)
- Detect delimiter (simple heuristic)
- Ensure required columns are present:
  - transaction_id, store_id, timestamp, item_id, quantity, unit_price, revenue
- If passes: copy object to `validated/`, delete from `raw/`
- If fails: copy object to `rejected/system/`, write `<file>_reason.json`, delete from `raw/`
- Publish SNS notification for failures or summary (optional)

Permissions required:
- s3:GetObject, s3:PutObject, s3:DeleteObject
- sns:Publish (if sending notifications)
