from pathlib import Path


# ---------------------------------------------------------
# configuration
# ---------------------------------------------------------

DATASETS = {
    "outcomes": Path(
        "/Volumes/workspace/uk_crime_bronze/"
        "raw_data_files/outcomes/backfill"
    ),
    "stop_search": Path(
        "/Volumes/workspace/uk_crime_bronze/"
        "raw_data_files/stop_search/backfill"
    )
}


# ---------------------------------------------------------
# audit extracted files
# ---------------------------------------------------------

for dataset_name, root_path in DATASETS.items():

    print("\n" + "=" * 60)
    print(f"VALIDATING: {dataset_name.upper()}")
    print("=" * 60)

    total_files = 0
    total_bytes = 0
    months_found = []

    for month_directory in sorted(root_path.iterdir()):

        if not month_directory.is_dir():
            continue

        csv_files = list(month_directory.glob("*.csv"))

        file_count = len(csv_files)

        size_bytes = sum(
            file.stat().st_size
            for file in csv_files
        )

        size_mb = size_bytes / (1024 * 1024)

        total_files += file_count
        total_bytes += size_bytes
        months_found.append(month_directory.name)

        print(
            f"{month_directory.name}: "
            f"{file_count} files | "
            f"{size_mb:.1f} MB"
        )

    print("\nSummary")
    print(f"Months found: {len(months_found)}")

    if months_found:
        print(f"Earliest month: {min(months_found)}")
        print(f"Latest month: {max(months_found)}")

    print(f"Total files: {total_files:,}")
    print(
        f"Total size: "
        f"{total_bytes / (1024 ** 3):.2f} GB"
    )

# ---------------------------------------------------------
# Inspect schemas
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("OUTCOMES SCHEMA")
print("=" * 60)

outcomes_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(
        "/Volumes/workspace/uk_crime_bronze/"
        "raw_data_files/outcomes/backfill/*/*.csv"
    )
)

outcomes_df.printSchema()

print("\nSample outcomes:")
outcomes_df.show(5, truncate=False)


print("\n" + "=" * 60)
print("STOP & SEARCH SCHEMA")
print("=" * 60)

stop_search_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(
        "/Volumes/workspace/uk_crime_bronze/"
        "raw_data_files/stop_search/backfill/*/*.csv"
    )
)

stop_search_df.printSchema()

print("\nSample stop & search:")
stop_search_df.show(5, truncate=False)