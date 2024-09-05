

import requests
import json
import pandas as pd


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


# Simplified GraphQL query
query = """
query ColumnLineage {
  publishedDatasources {
    name
    upstreamTables {
      name
      columns {
        name
        downstreamFields {
          name
        }
      }
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
    
    # Parse the JSON response
    result = response.json()
    
    if 'errors' in result:
        print("GraphQL Errors:")
        print(json.dumps(result['errors'], indent=2))
        raise ValueError("GraphQL query returned errors")
    
    data = result.get('data')
    
    if data is None:
        print("Error: No data returned from API")
        print("Full API response:")
        print(json.dumps(result, indent=2))
        raise ValueError("No data returned from API")
    
    # Process the data to create a flat structure for the lineage
    lineage_data = []
    for datasource in data['publishedDatasources']:
        for table in datasource['upstreamTables']:
            for column in table['columns']:
                for field in column['downstreamFields']:
                    lineage_data.append({
                        'Datasource': datasource['name'],
                        'Table': table['name'],
                        'Source Column': column['name'],
                        'Field': field['name']
                    })
    
    # Convert to a pandas DataFrame
    df = pd.DataFrame(lineage_data)
    
    # Save to CSV
    df.to_csv('column_lineage_tableau.csv', index=False)
    print("Column lineage data saved to 'column_lineage.csv'")
    
    # Display the first few rows
    print(df.head())

except requests.exceptions.RequestException as e:
    print(f"HTTP Request Error: {e}")
    if hasattr(e, 'response') and e.response is not None:
        print(f"Response content: {e.response.text}")
except ValueError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")