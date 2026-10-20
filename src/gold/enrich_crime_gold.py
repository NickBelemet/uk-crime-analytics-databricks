from pyspark.sql import functions as F

# --------------------------------------------------
# TABLE CONFIGURATION
# --------------------------------------------------

GOLD_SCHEMA = "workspace.uk_crime_gold"

SILVER_STREET_CRIME  = "workspace.uk_crime_silver.street_crime"
SILVER_OUTCOMES      = "workspace.uk_crime_silver.outcomes"
SILVER_STOP_SEARCH   = "workspace.uk_crime_silver.stop_search"
SILVER_POPULATION    = "workspace.uk_crime_silver.population_lsoa"
SILVER_DEPRIVATION   = "workspace.uk_crime_silver.deprivation_lsoa"

GOLD_STREET_CRIME_SUMMARY  = f"{GOLD_SCHEMA}.street_crime_summary"
GOLD_OUTCOMES_SUMMARY       = f"{GOLD_SCHEMA}.outcomes_summary"
GOLD_STOP_SEARCH_SUMMARY   = f"{GOLD_SCHEMA}.stop_search_summary"
GOLD_FORCE_ENRICHMENT       = f"{GOLD_SCHEMA}.force_enrichment"

POP_LATEST_YEAR = 2024  # latest ONS mid-year estimate available


# --------------------------------------------------
# 0. CREATE GOLD SCHEMA
# --------------------------------------------------

def create_schema():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    print(f"Schema ready: {GOLD_SCHEMA}")


# --------------------------------------------------
# WRITE HELPER — month-partitioned, automation-ready
# --------------------------------------------------

def write_gold_table(df, table):
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("partitionOverwriteMode", "dynamic")
          .partitionBy("month")
          .saveAsTable(table)
    )
    print(f"Successfully created: {table}")


# --------------------------------------------------
# 1. STREET CRIME SUMMARY
#    Grain: force_id x month x crime_type x lsoa_code
#    Coordinates: LSOA centroid (avg lat/lon per group)
#    Partitioned by: month
# --------------------------------------------------

def build_street_crime_summary():

    df = spark.table(SILVER_STREET_CRIME)

    summary = (
        df.groupBy("force_id", "month", "crime_type", "lsoa_code")
          .agg(
              F.count("*").alias("crime_count"),
              F.avg("latitude").alias("latitude"),
              F.avg("longitude").alias("longitude"),
              F.first("lsoa_name", ignorenulls=True).alias("lsoa_name"),
          )
          .orderBy("force_id", "month", "crime_type", "lsoa_code")
    )

    return summary


# --------------------------------------------------
# 2. OUTCOMES SUMMARY
#    Grain: force_id x month x crime_type x outcome_type x lsoa_code
#    Coordinates: LSOA centroid (avg lat/lon per group)
#    Partitioned by: month
# --------------------------------------------------

def build_outcomes_summary():

    outcomes = spark.table(SILVER_OUTCOMES)
    street   = spark.table(SILVER_STREET_CRIME)

    street_crime_types = (
        street.select("crime_id", "crime_type")
              .filter(F.col("crime_id").isNotNull())
              .dropDuplicates(["crime_id"])
    )

    joined = (
        outcomes.alias("o")
            .join(
                street_crime_types.alias("sc"),
                F.col("o.crime_id") == F.col("sc.crime_id"),
                "left"
            )
            .select(
                F.col("o.force_id").alias("force_id"),
                F.col("o.month").alias("month"),
                F.col("sc.crime_type").alias("crime_type"),
                F.col("o.outcome_type").alias("outcome_type"),
                F.col("o.lsoa_code").alias("lsoa_code"),
                F.col("o.lsoa_name").alias("lsoa_name"),
                F.col("o.latitude").alias("latitude"),
                F.col("o.longitude").alias("longitude"),
            )
    )

    summary = (
        joined.groupBy("force_id", "month", "crime_type",
                       "outcome_type", "lsoa_code")
              .agg(
                  F.count("*").alias("outcome_count"),
                  F.avg("latitude").alias("latitude"),
                  F.avg("longitude").alias("longitude"),
                  F.first("lsoa_name", ignorenulls=True).alias("lsoa_name"),
              )
              .orderBy("force_id", "month", "crime_type",
                       "outcome_type", "lsoa_code")
    )

    return summary


# --------------------------------------------------
# 3. STOP & SEARCH SUMMARY
#    Grain: force_id x month x object_of_search x lat_grid x lon_grid
#    Grid: lat/lon rounded to 2 decimal places (~1.1km cells)
#    Partitioned by: month
# --------------------------------------------------

def build_stop_search_summary():

    df = spark.table(SILVER_STOP_SEARCH)

    df = df.withColumns({
        "month":   F.to_date(F.date_trunc("month", F.col("date"))),
        "lat_grid": F.round(F.col("latitude"), 2),
        "lon_grid": F.round(F.col("longitude"), 2),
    })

    summary = (
        df.groupBy("force_id", "month", "object_of_search",
                   "lat_grid", "lon_grid")
          .agg(
              F.avg("latitude").alias("latitude"),
              F.avg("longitude").alias("longitude"),
              F.count("*").alias("search_count"),
              F.sum((F.col("outcome") == "Arrest")
                    .cast("long")).alias("arrest_count"),
              F.sum((F.col("outcome") == "A no further action disposal")
                    .cast("long")).alias("no_action_count"),
              F.sum((F.col("outcome") == "Community resolution")
                    .cast("long")).alias("community_resolution_count"),
              F.sum((F.col("outcome") == "Summons / charged by post")
                    .cast("long")).alias("summons_count"),
              F.sum((F.col("outcome") == "Penalty Notice for Disorder")
                    .cast("long")).alias("penalty_notice_count"),
              F.sum((F.col("outcome") == "Caution (simple or conditional)")
                    .cast("long")).alias("caution_count"),
              F.sum((F.col("gender") == "Male")
                    .cast("long")).alias("male_count"),
              F.sum((F.col("gender") == "Female")
                    .cast("long")).alias("female_count"),
              F.sum(F.col("age_range").isin("10-17", "under 10")
                    .cast("long")).alias("age_under_18_count"),
              F.sum(F.col("age_range").isin("18-24", "25-34")
                    .cast("long")).alias("age_18_34_count"),
              F.sum((F.col("age_range") == "over 34")
                    .cast("long")).alias("age_over_34_count"),
          )
          .orderBy("force_id", "month", "object_of_search",
                   "lat_grid", "lon_grid")
    )

    return summary


# --------------------------------------------------
# 4. VALIDATE GOLD TABLES
# --------------------------------------------------

from functools import reduce

def validate_gold():

    tables = [
        ("street_crime_summary", GOLD_STREET_CRIME_SUMMARY,
         ["force_id", "month", "crime_type", "lsoa_code"],
         "crime_count"),
        ("outcomes_summary", GOLD_OUTCOMES_SUMMARY,
         ["force_id", "month", "crime_type", "outcome_type", "lsoa_code"],
         "outcome_count"),
        ("stop_search_summary", GOLD_STOP_SEARCH_SUMMARY,
         ["force_id", "month", "object_of_search", "lat_grid", "lon_grid"],
         "search_count"),
    ]

    for label, table_name, key_cols, measure_col in tables:
        df = spark.table(table_name)
        total_rows = df.count()
        total_measure = df.agg(F.sum(measure_col)).first()[0]
        null_keys = df.filter(
            reduce(lambda a, b: a | b,
                   [F.col(c).isNull() for c in key_cols])
        ).count()
        with_coords = df.filter(
            F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
        ).count()
        coord_pct = (with_coords / total_rows * 100) if total_rows > 0 else 0
        distinct_forces = df.select("force_id").distinct().count()
        min_month = df.agg(F.min("month")).first()[0]
        max_month = df.agg(F.max("month")).first()[0]
        partitions = spark.sql(
            f"SHOW PARTITIONS {table_name}"
        ).count()

        print(f"\n--- {label} ---")
        print(f"  Rows:              {total_rows:,}")
        print(f"  Total {measure_col}: {total_measure:,}")
        print(f"  Distinct forces:   {distinct_forces}")
        print(f"  Date range:        {min_month} -> {max_month}")
        print(f"  Month partitions:  {partitions}")
        print(f"  With coordinates:  {with_coords:,} ({coord_pct:.1f}%)")
        print(f"  Null key cols:     {null_keys:,}")


# --------------------------------------------------
# 5. ENRICHMENT: POPULATION & DEPRIVATION
#
#    Builds a force-level enrichment table by joining:
#      a. LSOA -> force mapping (from street_crime)
#      b. ONS LSOA population (latest year, 2024)
#      c. IoD 2019 deprivation scores (89% LSOA match)
#    Aggregates to force level:
#      - total_population (sum)
#      - avg_imd_score (population-weighted)
#      - avg_imd_rank, avg_imd_decile (unweighted)
#      - Domain scores (population-weighted)
#    Table is NOT partitioned by month (force-level, static).
# --------------------------------------------------

def build_force_enrichment():

    # LSOA -> force mapping (one row per LSOA to avoid double-counting)
    lsoa_force = (
        spark.table(SILVER_STREET_CRIME)
            .select("lsoa_code", "force_id")
            .filter(F.col("lsoa_code").isNotNull())
            .dropDuplicates(["lsoa_code"])
    )

    # Population (latest available year)
    pop = (
        spark.table(SILVER_POPULATION)
            .filter(F.col("year") == POP_LATEST_YEAR)
            .select("lsoa_code", "total_population")
    )

    # Deprivation (key columns only)
    dep = (
        spark.table(SILVER_DEPRIVATION)
            .select(
                "lsoa_code", "imd_score", "imd_rank", "imd_decile",
                "income_score", "employment_score", "education_score",
                "health_score", "crime_score", "barriers_score",
                "living_env_score",
            )
    )

    # Join all at LSOA level
    lsoa_data = (
        lsoa_force
            .join(pop, "lsoa_code", "left")
            .join(dep, "lsoa_code", "left")
    )

    # Aggregate to force level
    enrichment = (
        lsoa_data.groupBy("force_id")
            .agg(
                F.sum("total_population").alias("total_population"),
                F.count("*").alias("lsoa_count"),
                F.sum(F.col("imd_score").isNotNull().cast("int"))
                    .alias("lsoa_with_deprivation"),

                # Weighted sums (score * population, only where both exist)
                F.sum(F.when(
                    F.col("imd_score").isNotNull(),
                    F.col("imd_score") * F.col("total_population")
                ).otherwise(0)).alias("_imd_w"),
                F.sum(F.when(
                    F.col("income_score").isNotNull(),
                    F.col("income_score") * F.col("total_population")
                ).otherwise(0)).alias("_income_w"),
                F.sum(F.when(
                    F.col("employment_score").isNotNull(),
                    F.col("employment_score") * F.col("total_population")
                ).otherwise(0)).alias("_employment_w"),
                F.sum(F.when(
                    F.col("education_score").isNotNull(),
                    F.col("education_score") * F.col("total_population")
                ).otherwise(0)).alias("_education_w"),
                F.sum(F.when(
                    F.col("health_score").isNotNull(),
                    F.col("health_score") * F.col("total_population")
                ).otherwise(0)).alias("_health_w"),
                F.sum(F.when(
                    F.col("crime_score").isNotNull(),
                    F.col("crime_score") * F.col("total_population")
                ).otherwise(0)).alias("_crime_w"),
                F.sum(F.when(
                    F.col("barriers_score").isNotNull(),
                    F.col("barriers_score") * F.col("total_population")
                ).otherwise(0)).alias("_barriers_w"),
                F.sum(F.when(
                    F.col("living_env_score").isNotNull(),
                    F.col("living_env_score") * F.col("total_population")
                ).otherwise(0)).alias("_living_env_w"),

                # Population where deprivation exists (denominator for weighting)
                F.sum(F.when(
                    F.col("imd_score").isNotNull(), F.col("total_population")
                ).otherwise(0)).alias("_dep_pop"),

                # Unweighted averages
                F.avg("imd_rank").alias("avg_imd_rank"),
                F.avg("imd_decile").alias("avg_imd_decile"),
            )
    )

    # Calculate population-weighted averages
    enrichment = enrichment.withColumns({
        "avg_imd_score":         F.col("_imd_w") / F.col("_dep_pop"),
        "avg_income_score":      F.col("_income_w") / F.col("_dep_pop"),
        "avg_employment_score":  F.col("_employment_w") / F.col("_dep_pop"),
        "avg_education_score":   F.col("_education_w") / F.col("_dep_pop"),
        "avg_health_score":      F.col("_health_w") / F.col("_dep_pop"),
        "avg_crime_score":       F.col("_crime_w") / F.col("_dep_pop"),
        "avg_barriers_score":    F.col("_barriers_w") / F.col("_dep_pop"),
        "avg_living_env_score":  F.col("_living_env_w") / F.col("_dep_pop"),
    })

    # Drop intermediate columns and select final output
    enrichment = enrichment.select(
        "force_id", "total_population", "lsoa_count",
        "lsoa_with_deprivation",
        "avg_imd_score", "avg_imd_rank", "avg_imd_decile",
        "avg_income_score", "avg_employment_score",
        "avg_education_score", "avg_health_score",
        "avg_crime_score", "avg_barriers_score",
        "avg_living_env_score",
    ).orderBy("force_id")

    return enrichment


def validate_enrichment():
    df = spark.table(GOLD_FORCE_ENRICHMENT)
    total_rows = df.count()
    print(f"\n--- force_enrichment ---")
    print(f"  Rows (forces):           {total_rows}")
    print(f"  Total population:        {df.agg(F.sum('total_population')).first()[0]:,.0f}")
    print(f"  Total LSOAs:             {df.agg(F.sum('lsoa_count')).first()[0]:,}")
    print(f"  LSOAs with deprivation:  {df.agg(F.sum('lsoa_with_deprivation')).first()[0]:,}")
    print(f"  Null population:         {df.filter(F.col('total_population').isNull()).count()}")
    print(f"  Null IMD score:          {df.filter(F.col('avg_imd_score').isNull()).count()}")

    print("\n  Top 5 forces by population:")
    df.orderBy(F.desc("total_population")).select(
        "force_id", "total_population", "lsoa_count",
        "lsoa_with_deprivation", "avg_imd_score", "avg_imd_decile"
    ).show(5, truncate=False)

    print("  Bottom 5 forces by IMD score (most deprived):")
    df.orderBy("avg_imd_score").select(
        "force_id", "total_population", "avg_imd_score",
        "avg_imd_decile", "avg_income_score", "avg_health_score"
    ).show(5, truncate=False)


# ------------------------------------------------
# 6. RUN GOLD PIPELINE
# ------------------------------------------------

print("=" * 60)
print("GOLD LAYER PIPELINE (geographic grain + month partitioning)")
print("=" * 60)

print("\nCreating gold schema...")
create_schema()

print("\n[1/4] Building street_crime_summary (LSOA grain)...")
street_crime_df = build_street_crime_summary()
print(f"  Rows: {street_crime_df.count():,}")
write_gold_table(street_crime_df, GOLD_STREET_CRIME_SUMMARY)

print("\n[2/4] Building outcomes_summary (LSOA grain)...")
outcomes_df = build_outcomes_summary()
print(f"  Rows: {outcomes_df.count():,}")
write_gold_table(outcomes_df, GOLD_OUTCOMES_SUMMARY)

print("\n[3/4] Building stop_search_summary (coordinate grid)...")
stop_search_df = build_stop_search_summary()
print(f"  Rows: {stop_search_df.count():,}")
write_gold_table(stop_search_df, GOLD_STOP_SEARCH_SUMMARY)

print("\n[4/4] Building force_enrichment (population + deprivation)...")
enrichment_df = build_force_enrichment()
print(f"  Rows: {enrichment_df.count():,}")
# Enrichment is force-level (not month-partitioned)
(
    enrichment_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_FORCE_ENRICHMENT)
)
print(f"Successfully created: {GOLD_FORCE_ENRICHMENT}")

print("\n" + "=" * 60)
print("VALIDATION")
print("=" * 60)
validate_gold()
validate_enrichment()

print("\n" + "=" * 60)
print("GOLD LAYER COMPLETE")
print("=" * 60)
