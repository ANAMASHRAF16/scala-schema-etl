"""
Generate sample Parquet files with DIFFERENT schemas to simulate schema evolution.

v1: [name, age, city]              — original schema
v2: [name, age, city, email]       — new column added
v3: [name, age, email, signup_date] — city removed, signup_date added
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("GenerateData").master("local[*]").getOrCreate()

BASE = os.path.join(os.path.dirname(__file__), "..", "data")

# --- v1: original schema [name, age, city] ---
v1_data = [
    ("Alice", 30, "New York"),
    ("Bob", 25, "London"),
    ("Charlie", 17, "Paris"),
]
v1_schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("city", StringType()),
])
spark.createDataFrame(v1_data, v1_schema).write.mode("overwrite").parquet(os.path.join(BASE, "v1"))
print("Created v1: [name, age, city]")

# --- v2: new column added [name, age, city, email] ---
v2_data = [
    ("Diana", 28, "Berlin", "diana@example.com"),
    ("Eve", 35, "Tokyo", "eve@example.com"),
    ("Frank", 16, "Sydney", "frank@example.com"),
]
v2_schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("city", StringType()),
    StructField("email", StringType()),
])
spark.createDataFrame(v2_data, v2_schema).write.mode("overwrite").parquet(os.path.join(BASE, "v2"))
print("Created v2: [name, age, city, email]")

# --- v3: column removed, new column added [name, age, email, signup_date] ---
v3_data = [
    ("Grace", 22, "grace@example.com", "2026-01-15"),
    ("Hank", 40, "hank@example.com", "2026-02-20"),
    ("Ivy", 19, "ivy@example.com", "2026-03-10"),
]
v3_schema = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("email", StringType()),
    StructField("signup_date", StringType()),
])
spark.createDataFrame(v3_data, v3_schema).write.mode("overwrite").parquet(os.path.join(BASE, "v3"))
print("Created v3: [name, age, email, signup_date]")

spark.stop()
print("\nDone. Three Parquet folders with different schemas.")
