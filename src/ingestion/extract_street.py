import zipfile
from pathlib import Path


# ---------------------------------------------------------
# config
# ---------------------------------------------------------

ZIP_PATH = Path(
    "/Volumes/workspace/uk_crime_bronze/"
    "raw_data_files/archive/2026-07.zip"
)

CRIME_PATH = Path(
    "/Volumes/workspace/uk_crime_bronze/"
    "raw_data_files/crime/backfill"
)


# ---------------------------------------------------------
# destination
# ---------------------------------------------------------

CRIME_PATH.mkdir(
    parents=True,
    exist_ok=True
)


# ---------------------------------------------------------
# extract street-crime files
# ---------------------------------------------------------

print("Opening Police.uk archive...")

with zipfile.ZipFile(ZIP_PATH, "r") as archive:

    # Find every street-crime CSV contained in the archive
    street_files = [
        name
        for name in archive.namelist()
        if name.endswith("-street.csv")
    ]

    print(
        f"Street crime files discovered in archive: "
        f"{len(street_files):,}"
    )

    extracted = 0
    skipped = 0

    for source_name in street_files:

        # Example:
        # 2023-08/2023-08-avon-and-somerset-street.csv

        month = source_name.split("/")[0]
        filename = Path(source_name).name

        month_directory = CRIME_PATH / month

        month_directory.mkdir(
            parents=True,
            exist_ok=True
        )

        destination = month_directory / filename

        # Get the expected size of the file inside the ZIP
        source_size = archive.getinfo(source_name).file_size

        # -------------------------------------------------
        # skip files already extracted successfully
        # -------------------------------------------------

        if (
            destination.exists()
            and destination.stat().st_size == source_size
        ):
            skipped += 1
            continue

        # -------------------------------------------------
        # extract missing or incomplete file
        # -------------------------------------------------

        with archive.open(source_name) as source:
            with destination.open("wb") as target:

                while True:

                    chunk = source.read(1024 * 1024)

                    if not chunk:
                        break

                    target.write(chunk)

        extracted += 1

        if extracted % 50 == 0:
            print(
                f"Extracted {extracted:,} "
                f"missing/incomplete files..."
            )


# ---------------------------------------------------------
# summary
# ---------------------------------------------------------

print("\nStreet crime extraction complete.")

print(
    f"Existing valid files skipped: "
    f"{skipped:,}"
)

print(
    f"Missing/incomplete files extracted: "
    f"{extracted:,}"
)

print(f"Destination: {CRIME_PATH}")