# Hardened Multi-Format Timestamp Parsing

Spark's `to_timestamp` can throw exceptions if a format partially matches. To avoid this:
- Use regex patterns to gate each format.
- Only call `to_timestamp` for strings that fully match the regex.

Supported formats (non-exhaustive):
- yyyy-MM-dd H:mm:ss
- yyyy-MM-dd H:mm
- yyyy/MM/dd H:mm:ss
- yyyy/MM/dd H:mm
- MM/dd/yyyy H:mm:ss
- MM/dd/yyyy H:mm
- dd/MM/yyyy H:mm:ss
- dd/MM/yyyy H:mm
- yyyyMMdd HHmmss
- yyyyMMdd
- yyyy-MM-dd
- yyyy/MM/dd
- dd/MM/yyyy
- MM/dd/yyyy

Implementation pattern (PySpark):
```python
parsed = lit(None)
for pattern, fmt in patterns:
    parsed = coalesce(parsed, when(col("timestamp_raw").rlike(pattern), to_timestamp(col("timestamp_raw"), fmt)))
df = df.withColumn("timestamp_parsed", parsed)
```
Rows with null `timestamp_parsed` are rejected as `INVALID_TIMESTAMP_FORMAT`.
