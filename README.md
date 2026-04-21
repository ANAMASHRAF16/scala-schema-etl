# Scala Schema ETL

A Spark ETL job that handles Parquet schema evolution without breaking downstream consumers.

## Problem

Source Parquet files evolve over time:
- v1: `[name, age, city]`
- v2: `[name, age, city, email]` — new column added
- v3: `[name, age, email, signup_date]` — column removed, new column added

The baseline ETL crashes when reading files with different schemas. Downstream consumers (dashboards, ML models) break because expected columns are missing.

## Fix

| Change | Before | After |
|---|---|---|
| Schema reading | Fails on mixed schemas | `mergeSchema=true` — unions all schemas |
| Missing columns | Crash / null | Default values (`unknown`, `0`, `1970-01-01`) |
| Null handling | No handling | `coalesce(col, default)` fills nulls |
| Output schema | Varies with input | Canonical schema — always same columns, same order |

## Architecture

```
data/v1/ (name, age, city)         ─┐
data/v2/ (name, age, city, email)  ─┼─> mergeSchema ─> ensure_canonical_schema() ─> output/
data/v3/ (name, age, email, date)  ─┘     (union)        (add defaults, fill nulls)
```

## Run (PySpark)

```bash
pip install pyspark

# Generate sample data (3 schema versions)
python pyspark/generate_data.py

# Run broken version (will fail)
python pyspark/etl_broken.py

# Run fixed version
python pyspark/etl_fixed.py

# Run tests
python pyspark/test_schema_compat.py
```

## Run (Scala)

```bash
sbt run -- data/ output/processed
```
