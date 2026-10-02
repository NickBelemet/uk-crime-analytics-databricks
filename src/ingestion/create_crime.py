from pyspark.sql.functions import (
    current_timestamp,
    col
)

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

SOURCE_PATH = (
    "/Volumes/workspace/uk_crime_bronze/"
    "raw_data_files/crime/test/*.json"
)

TARGET_TABLE = "workspace.uk_crime_bronze.crime"


# ---------------------------------------------------------
# Read raw JSON
# ---------------------------------------------------------

crime_df = (
    spark.read
    .option("multiline", "true")
    .json(SOURCE_PATH)
)

print(f"Raw records read: {crime_df.count():,}")

crime_df.printSchema()


# ---------------------------------------------------------
# Add ingestion metadata
# ---------------------------------------------------------

bronze_df = (
    crime_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)


# ---------------------------------------------------------
# Write Bronze Delta table
# ---------------------------------------------------------

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

print(f"Bronze table successfully created: {TARGET_TABLE}")
print(f"Rows written: {bronze_df.count():,}")