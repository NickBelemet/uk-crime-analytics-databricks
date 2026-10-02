import zipfile
from pathlib import Path
from collections import Counter


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

ZIP_PATH = Path(
    "/Volumes/workspace/uk_crime_bronze/"
    "raw_data_files/archive/2026-07.zip"
)


# ---------------------------------------------------------
# Inspect archive
# ---------------------------------------------------------

print(f"Inspecting: {ZIP_PATH}")

with zipfile.ZipFile(ZIP_PATH, "r") as archive:

    csv_files = [
        name
        for name in archive.namelist()
        if name.endswith(".csv")
    ]

    print(f"\nTotal CSV files: {len(csv_files):,}")

    # Identify months from the folder names
    months = sorted({
        name.split("/")[0]
        for name in csv_files
    })

    print(f"\nMonths discovered: {len(months)}")
    print(f"Earliest month: {months[0]}")
    print(f"Latest month:   {months[-1]}")

    # Classify dataset types
    street_files = [
        name for name in csv_files
        if name.endswith("-street.csv")
    ]

    outcome_files = [
        name for name in csv_files
        if name.endswith("-outcomes.csv")
    ]

    stop_search_files = [
        name for name in csv_files
        if name.endswith("-stop-and-search.csv")
    ]

    print("\nDataset files:")
    print(f"Street crime:    {len(street_files):,}")
    print(f"Outcomes:        {len(outcome_files):,}")
    print(f"Stop and search: {len(stop_search_files):,}")

    print("\nFirst 10 CSV files:")

    for name in csv_files[:10]:
        print(name)