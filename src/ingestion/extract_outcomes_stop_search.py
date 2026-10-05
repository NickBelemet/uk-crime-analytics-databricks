import zipfile
from pathlib import Path


# ---------------------------------------------------------
# nonfiguration
# ---------------------------------------------------------

ZIP_PATH = Path(
    "/Volumes/workspace/uk_crime_bronze/"
    "raw_data_files/archive/2026-07.zip"
)

OUTCOMES_PATH = Path(
    "/Volumes/workspace/uk_crime_bronze/"
    "raw_data_files/outcomes/backfill"
)

STOP_SEARCH_PATH = Path(
    "/Volumes/workspace/uk_crime_bronze/"
    "raw_data_files/stop_search/backfill"
)


# ---------------------------------------------------------
# directories
# ---------------------------------------------------------

OUTCOMES_PATH.mkdir(
    parents=True,
    exist_ok=True
)

STOP_SEARCH_PATH.mkdir(
    parents=True,
    exist_ok=True
)


# ---------------------------------------------------------
# opening archive
# ---------------------------------------------------------

print("Opening Police.uk archive...")

with zipfile.ZipFile(ZIP_PATH, "r") as archive:

    archive_files = archive.namelist()

    # Identify the two datasets
    outcome_files = [
        name
        for name in archive_files
        if name.endswith("-outcomes.csv")
    ]

    stop_search_files = [
        name
        for name in archive_files
        if name.endswith("-stop-and-search.csv")
    ]

    print(f"Outcome files discovered: {len(outcome_files):,}")
    print(f"Stop & search files discovered: {len(stop_search_files):,}")


    # -----------------------------------------------------
    # function for restartable extraction
    # -----------------------------------------------------

    def extract_files(file_list, destination_root, dataset_name):

        extracted = 0
        skipped = 0

        print(f"\nExtracting {dataset_name}...")

        for i, source_name in enumerate(file_list, start=1):

            # Example:
            # 2023-08/2023-08-avon-and-somerset-outcomes.csv

            month = source_name.split("/")[0]
            filename = Path(source_name).name

            month_directory = destination_root / month

            month_directory.mkdir(
                parents=True,
                exist_ok=True
            )

            destination = month_directory / filename

            # Skip files already successfully extracted
            if (
                destination.exists()
                and destination.stat().st_size > 0
            ):
                skipped += 1
                continue

            # Stream one file at a time
            with archive.open(source_name) as source:
                with destination.open("wb") as target:

                    while True:

                        chunk = source.read(1024 * 1024)

                        if not chunk:
                            break

                        target.write(chunk)

            extracted += 1

            if i % 100 == 0:
                print(
                    f"Processed {i:,} / "
                    f"{len(file_list):,} {dataset_name} files"
                )

        print(
            f"{dataset_name} complete | "
            f"extracted: {extracted:,} | "
            f"skipped: {skipped:,}"
        )


    # -----------------------------------------------------
    # Extract datasets
    # -----------------------------------------------------

    extract_files(
        outcome_files,
        OUTCOMES_PATH,
        "outcomes"
    )

    extract_files(
        stop_search_files,
        STOP_SEARCH_PATH,
        "stop & search"
    )


print("\nExtraction complete.")
print(f"Outcomes: {OUTCOMES_PATH}")
print(f"Stop & search: {STOP_SEARCH_PATH}")