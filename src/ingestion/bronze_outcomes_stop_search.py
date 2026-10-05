from pyspark.sql import functions as F


# =========================================================
# CONFIG
# =========================================================

OUTCOMES_PATH = (
    "/Volumes/workspace/uk_crime_bronze/"
    "raw_data_files/outcomes/backfill/*/*.csv"
)

STOP_SEARCH_PATH = (
    "/Volumes/workspace/uk_crime_bronze/"
    "raw_data_files/stop_search/backfill/*/*.csv"
)

OUTCOMES_TABLE = "workspace.uk_crime_bronze.outcomes"
STOP_SEARCH_TABLE = "workspace.uk_crime_bronze.stop_search"


# =========================================================
# OUTCOMEs
# =========================================================

print("Reading outcomes backfill files...")

outcomes_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(OUTCOMES_PATH)
)

# renaming source columns to Delta-friendly names
outcomes_df = (
    outcomes_df
    .withColumnRenamed("Crime ID", "crime_id")
    .withColumnRenamed("Month", "month")
    .withColumnRenamed("Reported by", "reported_by")
    .withColumnRenamed("Falls within", "falls_within")
    .withColumnRenamed("Longitude", "longitude")
    .withColumnRenamed("Latitude", "latitude")
    .withColumnRenamed("Location", "location")
    .withColumnRenamed("LSOA code", "lsoa_code")
    .withColumnRenamed("LSOA name", "lsoa_name")
    .withColumnRenamed("Outcome type", "outcome_type")
)

#ingestion metadata
outcomes_df = (
    outcomes_df
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_source_month", F.col("month"))
    .withColumn("_archive_version", F.lit("2026-07"))
    .withColumn("_ingested_at", F.current_timestamp())
)

print(f"Outcome records: {outcomes_df.count():,}")

print("Writing Bronze outcomes table...")

(
    outcomes_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(OUTCOMES_TABLE)
)

print(f"Created: {OUTCOMES_TABLE}")


# =========================================================
# STOP & SEARCH
# =========================================================

print("\nReading stop & search backfill files...")

stop_search_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(STOP_SEARCH_PATH)
)

#Delta-friendly names
stop_search_df = (
    stop_search_df
    .withColumnRenamed("Type", "type")
    .withColumnRenamed("Date", "date")
    .withColumnRenamed(
        "Part of a policing operation",
        "part_of_policing_operation"
    )
    .withColumnRenamed(
        "Policing operation",
        "policing_operation"
    )
    .withColumnRenamed("Latitude", "latitude")
    .withColumnRenamed("Longitude", "longitude")
    .withColumnRenamed("Gender", "gender")
    .withColumnRenamed("Age range", "age_range")
    .withColumnRenamed(
        "Self-defined ethnicity",
        "self_defined_ethnicity"
    )
    .withColumnRenamed(
        "Officer-defined ethnicity",
        "officer_defined_ethnicity"
    )
    .withColumnRenamed("Legislation", "legislation")
    .withColumnRenamed(
        "Object of search",
        "object_of_search"
    )
    .withColumnRenamed("Outcome", "outcome")
    .withColumnRenamed(
        "Outcome linked to object of search",
        "outcome_linked_to_object_of_search"
    )
    .withColumnRenamed(
        "Removal of more than just outer clothing",
        "removal_of_more_than_outer_clothing"
    )
)

#ingestion metadata
stop_search_df = (
    stop_search_df
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_month", F.regexp_extract(F.col("_metadata.file_path"),r"/(\d{4}-\d{2})/", 1))
    .withColumn("_archive_version", F.lit("2026-07"))
)

print(f"Stop & search records: {stop_search_df.count():,}")

print("Writing Bronze stop & search table...")

(
    stop_search_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(STOP_SEARCH_TABLE)
)

print(f"Created: {STOP_SEARCH_TABLE}")


print("\nBronze ingestion complete.")