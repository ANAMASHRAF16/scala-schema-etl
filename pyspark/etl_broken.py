"""
ETL Job — BROKEN version (PySpark equivalent of SchemaETL.scala).

PROBLEM: Reads Parquet with fixed schema expectations.
If source files have different schemas, this FAILS.
"""

import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL_Broken").master("local[*]").getOrCreate()

BASE = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "output", "broken")

# Try to read ALL versions together — this FAILS because schemas differ
try:
    df = spark.read.parquet(
        os.path.join(BASE, "v1"),
        os.path.join(BASE, "v2"),
        os.path.join(BASE, "v3"),
    )
    print(f"Schema: {df.columns}")
    print(f"Count: {df.count()}")

    # Hardcoded column selection — breaks if columns don't exist
    processed = df.filter(df["age"] >= 18).select("name", "age", "city")
    processed.write.mode("overwrite").parquet(OUTPUT)
    print(f"Wrote {processed.count()} records")

except Exception as e:
    print(f"FAILED: {e}")
    print("The ETL crashed because Parquet files have different schemas.")

spark.stop()
