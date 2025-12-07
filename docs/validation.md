# Data Quality & Validation Rules

## Structural Validation
A row is structurally good only if **all required columns exist** (mapped via header). Required columns:
- transaction_id
- store_id
- timestamp
- item_id
- quantity
- unit_price
- revenue

Structural rejects are written with `reject_reason = "MISSING_REQUIRED_COLUMN"`.

## Business Rules (DQ)
- `timestamp` must be parseable
- `revenue` ≈ `quantity * unit_price` (tolerance 0.01)
- `quantity` and `unit_price` must be positive where applicable

Rows failing business logic are labelled `BUSINESS_LOGIC_FAIL`.

## Reject Outputs
- Machine-readable JSON: `rejected/data_quality/json/`
- Analyst-friendly CSV: `rejected/data_quality/csv/` (coalesced)
