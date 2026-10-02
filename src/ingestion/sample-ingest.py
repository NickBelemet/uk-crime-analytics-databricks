import requests
import json
from pathlib import Path

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

API_URL = "https://data.police.uk/api/crimes-street/all-crime"

# Small test location: central Birmingham
PARAMS = {
    "lat": 52.4862,
    "lng": -1.8904,
    "date": "2026-07"
}

VOLUME_PATH = Path(
    "/Volumes/workspace/uk_crime_bronze/raw_data_files/"
    "crime/test"
)

OUTPUT_FILE = VOLUME_PATH / "birmingham_2026-07.json"


# ---------------------------------------------------------
# Extract
# ---------------------------------------------------------

print("Requesting sample crime data from Police.uk...")

response = requests.get(
    API_URL,
    params=PARAMS,
    timeout=30
)

response.raise_for_status()

crime_data = response.json()

print(f"Received {len(crime_data):,} crime records.")


# ---------------------------------------------------------
# Load into Bronze landing volume
# ---------------------------------------------------------

VOLUME_PATH.mkdir(
    parents=True,
    exist_ok=True
)

with OUTPUT_FILE.open("w") as file:
    json.dump(crime_data, file)

print("Raw data successfully written to:")
print(OUTPUT_FILE)