


import requests
import json

# Your Tableau Online site URL (replace with your actual URL)
site_url = "https://10ay.online.tableau.com"  # Replace '10ay' with your actual pod

# Your authentication token (replace with the token you received from the signin process)
auth_token = 'pTnP78TGQRins9dq7h6S8g|UGNTr0R5hfko5e6Bd40wPQX39VtLLFV1|8c57b715-434e-4e36-8739-404ca15d4a20'

# Metadata API endpoint
metadata_url = f"{site_url}/api/metadata/graphql"

# Headers for the request
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-Tableau-Auth": auth_token
}

# A simple GraphQL query to test our access
# This query will return information about all dashboards
query = """
{
  dashboards {
    id
    name
    workbook {
      name
    }
  }
}
"""

# Prepare the request payload
payload = {
    "query": query
}

try:
    # Make the POST request to the Metadata API
    response = requests.post(metadata_url, json=payload, headers=headers)

    # Check if the request was successful
    response.raise_for_status()

    # Print the JSON response
    print(json.dumps(response.json(), indent=2))

except requests.exceptions.RequestException as e:
    print(f"Error: {e}")
    if hasattr(e, 'response') and e.response is not None:
        print(f"Response content: {e.response.content}")