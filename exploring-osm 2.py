import requests
import json

# Define the Overpass query
query = """
    [bbox:30.618338,-96.323712,30.591028,-96.330826]
    [out:json]
    [timeout:90]
    ;
    (
        way
            (
                 30.626917110746,
                 -96.348809105664,
                 30.634468750236,
                 -96.339893442898
             );
    );
    out geom;
"""

# Prepare the URL and POST data
url = "https://overpass-api.de/api/interpreter"
payload = {"data": query}

# Make the POST request
response = requests.post(url, data=payload)

# Check for a successful response
if response.status_code == 200:
    result = response.json()
    # Print the formatted JSON response
    print("Extract complete")
else:
    print(f"Error: {response.status_code}")
    print(response.text)

# Save the JSON to a file
with open("output.json", "w") as f:
    json.dump(result, f, indent=2)
