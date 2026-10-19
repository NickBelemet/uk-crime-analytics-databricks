from pyspark.sql import functions as F

# --------------------------------------------------
# TABLE CONFIGURATION
# --------------------------------------------------

BRONZE_TABLE = "workspace.uk_crime_bronze.outcomes"
SILVER_TABLE = "workspace.uk_crime_silver.outcomes"


# --------------------------------------------------
# 1. READ BRONZE
# --------------------------------------------------

def load_bronze():
    return spark.table(BRONZE_TABLE)


# --------------------------------------------------
# 2. TRANSFORM DATA
# --------------------------------------------------

def transform_outcomes(df):

    # Remove redundant columns
    df = df.drop(
        "reported_by",
        "_source_month",
        "_archive_version",
        "_ingested_at"
    )

    # Trim whitespace on key categorical / text columns
    string_cols = [
        "falls_within",
        "location",
        "lsoa_code",
        "lsoa_name",
        "outcome_type",
    ]

    # Apply all type conversions, trims, and force_id extraction
    # in a single withColumns call to avoid deeply nested plans
    df = df.withColumns({
        "month": F.to_date(
            F.concat(F.col("month"), F.lit("-01")),
            "yyyy-MM-dd"
        ),
        "longitude": F.col("longitude").cast("double"),
        "latitude": F.col("latitude").cast("double"),
        **{c: F.trim(F.col(c)) for c in string_cols},
        "force_id": F.regexp_extract(
            F.col("_source_file"),
            r"\d{4}-\d{2}-(.*?)-outcomes\.csv$",
            1
        ),
    })

    return df


# --------------------------------------------------
# 3. REMOVE IDENTIFIABLE EXACT DUPLICATES
# --------------------------------------------------

def remove_duplicates(df):

    with_id = df.filter(F.col("crime_id").isNotNull())
    without_id = df.filter(F.col("crime_id").isNull())

    with_id_clean = with_id.dropDuplicates()

    return with_id_clean.unionByName(without_id)


# --------------------------------------------------
# 4. VALIDATE SILVER
# --------------------------------------------------

def validate_silver(df):

    checks = df.agg(
        F.sum(F.col("month").isNull().cast("long")).alias("null_months"),
        F.sum((F.col("force_id").isNull() |
               (F.col("force_id") == "")).cast("long")).alias("invalid_force_ids"),
        F.count("*").alias("total_records")
    ).first()

    print(f"Total Silver records: {checks['total_records']:,}")
    print(f"NULL months: {checks['null_months']:,}")
    print(f"Invalid force IDs: {checks['invalid_force_ids']:,}")

    if checks["null_months"] > 0 or checks["invalid_force_ids"] > 0:
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
silver_df = transform_outcomes(bronze_df)

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
        "month",
        "falls_within",
        "force_id",
        "longitude",
        "latitude",
        "outcome_type"
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