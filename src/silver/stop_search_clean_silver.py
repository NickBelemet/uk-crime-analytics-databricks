from pyspark.sql import functions as F

# --------------------------------------------------
# TABLE CONFIGURATION
# --------------------------------------------------

BRONZE_TABLE = "workspace.uk_crime_bronze.stop_search"
SILVER_TABLE = "workspace.uk_crime_silver.stop_search"


# --------------------------------------------------
# 1. READ BRONZE
# --------------------------------------------------

def load_bronze():
    return spark.table(BRONZE_TABLE)


# --------------------------------------------------
# 2. TRANSFORM DATA
# --------------------------------------------------

def transform_stop_search(df):

    # Drop redundant / empty columns
    df = df.drop(
        "policing_operation",
        "_source_month",
        "_archive_version",
        "_ingested_at"
    )

    # Trim whitespace on key categorical / text columns
    string_cols = [
        "type",
        "gender",
        "age_range",
        "self_defined_ethnicity",
        "officer_defined_ethnicity",
        "legislation",
        "object_of_search",
        "outcome",
    ]

    # Apply all type conversions, boolean casts, trims, and force_id
    # extraction in a single withColumns call to avoid deeply nested plans
    df = df.withColumns({
        "date": F.col("date").cast("timestamp"),
        "latitude": F.col("latitude").cast("double"),
        "longitude": F.col("longitude").cast("double"),
        "part_of_policing_operation": F.col("part_of_policing_operation").cast("boolean"),
        "outcome_linked_to_object_of_search": F.col("outcome_linked_to_object_of_search").cast("boolean"),
        "removal_of_more_than_outer_clothing": F.col("removal_of_more_than_outer_clothing").cast("boolean"),
        **{c: F.trim(F.col(c)) for c in string_cols},
        "force_id": F.regexp_extract(
            F.col("_source_file"),
            r"\d{4}-\d{2}-(.*?)-stop-and-search\.csv$",
            1
        ),
    })

    return df


# --------------------------------------------------
# 3. REMOVE EXACT DUPLICATES
# --------------------------------------------------

def remove_duplicates(df):
    return df.dropDuplicates()


# --------------------------------------------------
# 4. VALIDATE SILVER
# --------------------------------------------------

def validate_silver(df):

    checks = df.agg(
        F.sum(F.col("date").isNull().cast("long")).alias("null_dates"),
        F.sum(
            (F.col("force_id").isNull() | (F.col("force_id") == ""))
            .cast("long")
        ).alias("invalid_force_ids"),
        F.count("*").alias("total_records")
    ).first()

    print(f"Total Silver records: {checks['total_records']:,}")
    print(f"NULL dates: {checks['null_dates']:,}")
    print(f"Invalid force IDs: {checks['invalid_force_ids']:,}")

    if checks["null_dates"] > 0 or checks["invalid_force_ids"] > 0:
        raise ValueError("Silver validation failed.")


# --------------------------------------------------
# 5. WRITE SILVER DELTA TABLE
# --------------------------------------------------

def write_silver(df):

    # Remove temporary source filename
    final_df = df.drop("_source_file")

    # Prevent accidental replacement of an existing table
    if spark.catalog.tableExists(SILVER_TABLE):
        raise RuntimeError(
            f"{SILVER_TABLE} already exists. "
            "Review it before replacing."
        )

    (
        final_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_TABLE)
    )

    print(f"Successfully created: {SILVER_TABLE}")


# --------------------------------------------------
# 6. RUN SILVER PIPELINE (VALIDATION ONLY)
# --------------------------------------------------

print("Loading Bronze data...")
bronze_df = load_bronze()

print("Applying Silver transformations...")
silver_df = transform_stop_search(bronze_df)

print("Removing exact duplicates...")
silver_df = remove_duplicates(silver_df)

print("Validating Silver data...")
validate_silver(silver_df)

print("Silver validation complete!")

# ------------------------------------------------
# 7. INSPECT
# ------------------------------------------------

silver_df.printSchema()

display(
    silver_df.select(
        "date",
        "type",
        "force_id",
        "latitude",
        "longitude",
        "gender",
        "age_range",
        "outcome",
        "object_of_search",
        "legislation"
    ).limit(10)
)

# Check for coordinates outside plausible UK ranges
invalid_geo = silver_df.filter(
    (
        F.col("longitude").isNotNull()
        & ~F.col("longitude").between(-9, 3)
    )
    |
    (
        F.col("latitude").isNotNull()
        & ~F.col("latitude").between(49, 61)
    )
)

print("Coordinates outside expected UK range:", invalid_geo.count())

# Write the validated DataFrame
write_silver(silver_df)