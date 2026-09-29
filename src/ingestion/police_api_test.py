import requests

# Police.uk endpoint that reports the latest available crime data
API_URL = "https://data.police.uk/api/crime-last-updated"

response = requests.get(API_URL, timeout=30)

# Raise an error if the request was unsuccessful
response.raise_for_status()

data = response.json()

print("Police.uk API connection successful")
print(f"Latest available crime data: {data['date']}")