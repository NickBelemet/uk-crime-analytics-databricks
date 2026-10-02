from pyspark.sql.functions import (
    current_timestamp,
    col,
    lit
)

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

SOURCE_PATH = (
    "/Volumes/workspace/uk_crime_bronze/"
    "raw_data_files/crime/backfill/*/*.csv"
)

TARGET_TABLE = "workspace.uk_crime_bronze.street_crime"


# ---------------------------------------------------------
# Read street crime CSV files
# ---------------------------------------------------------

print("Reading street crime backfill files...")

street_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(SOURCE_PATH)
)

print("Files successfully read.")


# ---------------------------------------------------------
# Standardise column names
# ---------------------------------------------------------

column_mapping = {
    "Crime ID": "crime_id",
    "Month": "month",
    "Reported by": "reported_by",
    "Falls within": "falls_within",
    "Longitude": "longitude",
    "Latitude": "latitude",
    "Location": "location",
    "LSOA code": "lsoa_code",
    "LSOA name": "lsoa_name",
    "Crime type": "crime_type",
    "Last outcome category": "last_outcome_category",
    "Context": "context"
}

for old_name, new_name in column_mapping.items():
    street_df = street_df.withColumnRenamed(
        old_name,
        new_name
    )


# ---------------------------------------------------------
# Add Bronze lineage metadata
# ---------------------------------------------------------

bronze_df = (
    street_df
    .withColumn(
        "_source_file",
        col("_metadata.file_path")
    )
    .withColumn(
        "_source_month",
        col("month")
    )
    .withColumn(
        "_archive_version",
        lit("2026-07")
    )
    .withColumn(
        "_ingested_at",
        current_timestamp()
    )
)


# ---------------------------------------------------------
# Validate
# ---------------------------------------------------------

record_count = bronze_df.count()

print(
    f"\nTotal street crime records: "
    f"{record_count:,}"
)

print("\nMonths discovered:")

(
    bronze_df
    .groupBy("_source_month")
    .count()
    .orderBy("_source_month")
    .show(50, truncate=False)
)


# ---------------------------------------------------------
# Write Bronze Delta table
# ---------------------------------------------------------

print("\nWriting Bronze Delta table...")

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

print(
    "\nBronze table successfully created: "
    f"{TARGET_TABLE}"
)

print(
    f"Rows written: "
    f"{record_count:,}"
)