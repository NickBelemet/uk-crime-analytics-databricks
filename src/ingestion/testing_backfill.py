from pathlib import Path

BACKFILL_PATH = Path(
    "/Volumes/workspace/uk_crime_bronze/"
    "raw_data_files/crime/backfill"
)

print("Checking extracted monthly folders...\n")

for month_dir in sorted(BACKFILL_PATH.iterdir()):

    if month_dir.is_dir():

        csv_files = list(
            month_dir.glob("*-street.csv")
        )

        total_size_mb = sum(
            file.stat().st_size
            for file in csv_files
        ) / (1024 ** 2)

        print(
            f"{month_dir.name}: "
            f"{len(csv_files)} files | "
            f"{total_size_mb:.1f} MB"
        )