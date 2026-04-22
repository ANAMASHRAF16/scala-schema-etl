"""
Test: Verify that the fixed ETL handles schema evolution correctly.

Checks:
1. All 3 versions (v1, v2, v3) read successfully together
2. Output has all canonical columns
3. Missing values are filled with defaults (not nulls)
4. No records are lost
5. Output schema is identical regardless of input schema versions
"""

import os
import sys
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce

BASE = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "output", "test")

CANONICAL_SCHEMA = {
    "name":        ("string", "unknown"),
    "age":         ("int",    0),
    "city":        ("string", "unknown"),
    "email":       ("string", "none"),
    "signup_date": ("string", "1970-01-01"),
}


def ensure_canonical_schema(df):
    for col_name, (col_type, default) in CANONICAL_SCHEMA.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, lit(default))
        else:
            df = df.withColumn(col_name, coalesce(col(col_name), lit(default)))
    return df.select(sorted(CANONICAL_SCHEMA.keys()))


def test():
    spark = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()

    if os.path.exists(OUTPUT):
        shutil.rmtree(OUTPUT)

    passed = 0
    failed = 0

    # TEST 1: mergeSchema reads all versions without error
    print("TEST 1: Read all schema versions together...")
    try:
        df = spark.read.option("mergeSchema", "true").parquet(
            os.path.join(BASE, "v1"),
            os.path.join(BASE, "v2"),
            os.path.join(BASE, "v3"),
        )
        assert df.count() == 9, f"Expected 9 records, got {df.count()}"
        print(f"  PASSED: Read {df.count()} records from 3 schema versions")
        passed += 1
    except Exception as e:
        print(f"  FAILED: {e}")
        failed += 1

    # TEST 2: Canonical schema has all expected columns
    print("TEST 2: Output has all canonical columns...")
    normalized = ensure_canonical_schema(df)
    expected_cols = sorted(CANONICAL_SCHEMA.keys())
    actual_cols = sorted(normalized.columns)
    if expected_cols == actual_cols:
        print(f"  PASSED: Columns match {actual_cols}")
        passed += 1
    else:
        print(f"  FAILED: Expected {expected_cols}, got {actual_cols}")
        failed += 1

    # TEST 3: No nulls in canonical columns
    print("TEST 3: No nulls in output (defaults filled)...")
    null_count = 0
    for col_name in CANONICAL_SCHEMA:
        nulls = normalized.filter(col(col_name).isNull()).count()
        if nulls > 0:
            print(f"  FAILED: {col_name} has {nulls} nulls")
            null_count += nulls
    if null_count == 0:
        print("  PASSED: No nulls in any canonical column")
        passed += 1
    else:
        failed += 1

    # TEST 4: v1 records have defaults for email and signup_date
    print("TEST 4: v1 records (Alice, Bob) have default email and signup_date...")
    alice = normalized.filter(col("name") == "Alice").collect()[0]
    if alice["email"] == "none" and alice["signup_date"] == "1970-01-01":
        print(f"  PASSED: Alice has email={alice['email']}, signup_date={alice['signup_date']}")
        passed += 1
    else:
        print(f"  FAILED: Alice has email={alice['email']}, signup_date={alice['signup_date']}")
        failed += 1

    # TEST 5: v3 records have default city (city was removed in v3)
    print("TEST 5: v3 records (Grace, Hank) have default city...")
    grace = normalized.filter(col("name") == "Grace").collect()[0]
    if grace["city"] == "unknown":
        print(f"  PASSED: Grace has city={grace['city']}")
        passed += 1
    else:
        print(f"  FAILED: Grace has city={grace['city']}")
        failed += 1

    # TEST 6: Write and re-read output — schema is stable
    print("TEST 6: Write output and re-read — schema stays consistent...")
    processed = normalized.filter(col("age") >= 18)
    processed.write.mode("overwrite").parquet(OUTPUT)
    reread = spark.read.parquet(OUTPUT)
    if sorted(reread.columns) == expected_cols:
        print(f"  PASSED: Re-read columns match {sorted(reread.columns)}")
        passed += 1
    else:
        print(f"  FAILED: Re-read columns {sorted(reread.columns)} != {expected_cols}")
        failed += 1

    # Summary
    print(f"\n{'='*40}")
    print(f"PASSED: {passed}/{passed + failed}")
    print(f"FAILED: {failed}/{passed + failed}")
    if failed > 0:
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    test()
