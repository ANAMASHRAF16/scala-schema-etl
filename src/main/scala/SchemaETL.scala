/**
 * Schema ETL — FIXED version.
 *
 * Fixes:
 * 1. Schema merging: reads all Parquet files and merges their schemas (union of all columns)
 * 2. Default values: missing columns are filled with sensible defaults (not null)
 * 3. Canonical schema: output always has a guaranteed set of columns, regardless of input
 * 4. Backward compatible: downstream consumers always see the same schema
 */

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, coalesce}
import org.apache.spark.sql.types.{StringType, IntegerType}

object SchemaETL {

  // Canonical schema: the columns downstream consumers ALWAYS expect.
  // If a column is missing from the input, we add it with a default value.
  val CANONICAL_COLUMNS: Map[String, (String, Any)] = Map(
    "name"        -> ("string", "unknown"),
    "age"         -> ("int", 0),
    "city"        -> ("string", "unknown"),
    "email"       -> ("string", "none"),
    "signup_date" -> ("string", "1970-01-01")
  )

  def ensureCanonicalSchema(df: DataFrame): DataFrame = {
    // For each expected column: if it exists, keep it. If missing, add it with default.
    var result = df

    CANONICAL_COLUMNS.foreach { case (colName, (colType, defaultVal)) =>
      if (!df.columns.contains(colName)) {
        // Column missing — add it with default value
        colType match {
          case "string" => result = result.withColumn(colName, lit(defaultVal.toString))
          case "int"    => result = result.withColumn(colName, lit(defaultVal.asInstanceOf[Int]))
        }
      } else {
        // Column exists but might have nulls — fill nulls with default
        colType match {
          case "string" => result = result.withColumn(colName, coalesce(col(colName), lit(defaultVal.toString)))
          case "int"    => result = result.withColumn(colName, coalesce(col(colName), lit(defaultVal.asInstanceOf[Int])))
        }
      }
    }

    // Select only canonical columns in consistent order
    result.select(CANONICAL_COLUMNS.keys.toSeq.sorted.map(col): _*)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SchemaETL")
      .master("local[*]")
      .getOrCreate()

    val inputPath = args(0)
    val outputPath = args(1)

    // FIX 1: mergeSchema reads all files and unions their schemas
    val df = spark.read
      .option("mergeSchema", "true")
      .parquet(inputPath)

    println(s"Merged schema: ${df.schema.fieldNames.mkString(", ")}")
    println(s"Total records: ${df.count()}")

    // FIX 2: Ensure canonical schema with defaults for missing columns
    val normalized = ensureCanonicalSchema(df)

    // FIX 3: Filter and write — downstream always sees the same columns
    val processed = normalized.filter(col("age") >= 18)

    processed.write
      .mode("overwrite")
      .parquet(outputPath)

    println(s"Wrote ${processed.count()} records with canonical schema")
    println(s"Output columns: ${processed.schema.fieldNames.mkString(", ")}")

    spark.stop()
  }
}
