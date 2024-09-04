
import requests
import json

# Tableau Online API endpoint
auth_url = "https://10ay.online.tableau.com/api/3.13/auth/signin"  # Replace '10ay' with your actual pod

# Tableau Online personal access token
personal_access_token_name = "MetadataConnect"
personal_access_token_secret = "0gvz1QgzQoScDLtCgDEQSA==:7lztd7ywtsxQuQObszW398fLdKEXBxy2"

# Set up authentication payload
auth_payload = {
    "credentials": {
        "personalAccessTokenName": personal_access_token_name,
        "personalAccessTokenSecret": personal_access_token_secret,
        "site": {
            "contentUrl": "murtuzahm100d709e8c"  # Replace with your actual site name
        }
    }
}

# Set up headers
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

try:
    # Make a POST request to sign in
    response = requests.post(auth_url, json=auth_payload, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        print("Tableau Online API authentication successful!")
        
        # Print the JSON response
        print(json.dumps(response.json(), indent=2))

        # Extract the auth token
        auth_token = response.json()["credentials"]["token"]
        print(f"Auth Token: {auth_token}")

        # You can now use this auth_token for subsequent API calls
    else:
        print(f"Error accessing Tableau Online API: {response.status_code} - {response.text}")
except requests.exceptions.RequestException as e:
    print(f"Error: {e}")