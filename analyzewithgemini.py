import json
import csv

import google.generativeai as genai
from typing import Dict, List, Any
from tenacity import retry, stop_after_attempt, wait_fixed

# Replace with your actual API key
GEMINI_API_KEY = "AIzaSyAyXykU1ici5JhgReve_ODXlvtUZNE4CXs"


# Configure the Gemini API
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-pro')

def load_json_data(file_path: str) -> Dict[str, Any]:
    """Loads a JSON file from the specified path.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict: The loaded JSON data as a dictionary.
    """

    with open(file_path, 'r') as f:
        return json.load(f)

def get_column_details(model_data: Dict[str, Any], column_name: str) -> Dict[str, Any]:
    """Extracts column details from a model data dictionary.

    Args:
        model_data (dict): A dictionary containing model data.
        column_name (str): The name of the column.

    Returns:
        dict: A dictionary containing column details.
    """

    return {
        'model': model_data['name'],
        'column': column_name,
        'column_type': model_data['columns'][column_name]['type'],
        'upstream_models': [
            {
                'id': model['id'],
                'name': model['name'],
                'columns': model['columns']
            }
            for model in model_data.get('upstream_models', [])
        ]
    }

def query_gemini(column_details: Dict[str, Any]) -> str:
    """Queries Gemini for lineage information based on column details.

    Args:
        column_details (dict): A dictionary containing column details.

    Returns:
        str: The Gemini response.
    """

    prompt = f"""
    Analyze the following column and determine its likely upstream column(s) based on the information provided:

    Model: {column_details['model']}
    Column: {column_details['column']}
    Column Type: {column_details['column_type']}
    Upstream Models and their columns:
    {json.dumps(column_details['upstream_models'], indent=2)}

    Please identify the most likely upstream column(s) that were used to create this column.
    Explain your reasoning briefly. If you can't determine a direct lineage, provide your best guess
    based on naming conventions and typical data transformations.

    Response Format:
    Upstream Column(s): [list the likely upstream column(s)]
    Reasoning: [brief explanation]
    """

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(60))  # Retry 5 times, waiting 60 seconds each time
    def retryable_query_gemini():
        return model.generate_content(prompt)

    response = retryable_query_gemini()
    return response.text

def analyze_lineage(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Analyzes lineage information for each column in the given data.

    Args:
        data (dict): A dictionary containing lineage information.

    Returns:
        list: A list of dictionaries, each containing information about a model and its columns.
    """

    results = []
    for model_name, model_data in data.items():
        if model_data['type'] == 'model':
            for column_name, column_info in model_data['columns'].items():
                column_details = get_column_details(model_data, column_name)

                @retry(stop=stop_after_attempt(5), wait=wait_fixed(60))  # Retry 5 times, waiting 60 seconds each time
                def query_gemini_with_retry(column_details):
                    return query_gemini(column_details)

                try:
                    gemini_response = query_gemini_with_retry(column_details)
                    column_details['gemini_analysis'] = gemini_response
                    results.append(column_details)
                    print(gemini_response)
                except Exception as e:
                    print(f"Error analyzing column {column_name} in model {model_name}: {e}")
                    print(f"Gemini query input: {column_details}")

    return results

def main():
    """Main function to load data, analyze lineage, and save results."""

    data = load_json_data('dbt_lineage_jaffle_shop.json')
    results = analyze_lineage(data)

    with open('lineage_analysis_results.json', 'w') as f:
        json.dump(results, f, indent=2)

    # Convert results to CSV
    csv_data = []
    for result in results:
        csv_data.append({
            'model': result['model'],
            'column': result['column'],
            'column_type': result['column_type'],
            'upstream_models': result['upstream_models'],
            'gemini_analysis': result['gemini_analysis']
        })

    with open('lineage_analysis_results.csv', 'w', newline='') as csvfile:
        fieldnames = ['model', 'column', 'column_type', 'upstream_models', 'gemini_analysis']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_data)

    print("Analysis complete. Results saved to lineage_analysis_results.json and lineage_analysis_results.csv")

if __name__ == "__main__":
    main()