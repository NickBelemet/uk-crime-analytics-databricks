import requests
import zipfile
from pathlib import Path


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

ARCHIVE_VERSION = "2026-07"

ARCHIVE_URL = (
    f"https://data.police.uk/data/archive/"
    f"{ARCHIVE_VERSION}.zip"
)

ARCHIVE_DIR = Path(
    "/Volumes/workspace/uk_crime_bronze/"
    "raw_data_files/archive"
)

ZIP_PATH = ARCHIVE_DIR / f"{ARCHIVE_VERSION}.zip"


# ---------------------------------------------------------
# Create archive directory
# ---------------------------------------------------------

ARCHIVE_DIR.mkdir(
    parents=True,
    exist_ok=True
)

print(f"Archive directory: {ARCHIVE_DIR}")


# ---------------------------------------------------------
# Download archive
# ---------------------------------------------------------

print("\nStarting Police.uk archive download...")
print(f"Archive: {ARCHIVE_URL}")

with requests.get(
    ARCHIVE_URL,
    stream=True,
    timeout=(30, 600)
) as response:

    response.raise_for_status()

    total_bytes = 0

    with ZIP_PATH.open("wb") as file:

        for chunk in response.iter_content(
            chunk_size=1024 * 1024
        ):
            if chunk:
                file.write(chunk)
                total_bytes += len(chunk)

                print(
                    f"\rDownloaded: "
                    f"{total_bytes / (1024 ** 3):.2f} GB",
                    end=""
                )


print("\n\nDownload complete.")

archive_size = ZIP_PATH.stat().st_size / (1024 ** 3)

print(f"Archive size: {archive_size:.2f} GB")
print(f"Saved to: {ZIP_PATH}")


# ---------------------------------------------------------
# Inspect archive — DO NOT extract yet
# ---------------------------------------------------------

print("\nInspecting archive...")

with zipfile.ZipFile(ZIP_PATH, "r") as archive:

    file_names = archive.namelist()

    print(
        f"Files/directories found: "
        f"{len(file_names):,}"
    )

    print("\nFirst 20 archive entries:")

    for name in file_names[:20]:
        print(name)