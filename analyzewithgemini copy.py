import json
import csv
import google.generativeai as genai
from typing import Dict, List, Any
from tenacity import retry, stop_after_attempt, wait_fixed
import dotenv
from os import environ

dotenv.load_dotenv()

GEMINI_API_KEY = environ.get("MY_API_KEY")

# Configure the Gemini API
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-pro')

def load_json_data(file_path: str) -> Dict[str, Any]:
    with open(file_path, 'r') as f:
        return json.load(f)

def get_column_details(model_data: Dict[str, Any], column_name: str) -> Dict[str, Any]:
    column_info = model_data['columns'][column_name]
    return {
        'model': model_data['name'],
        'column': column_name,
        'column_type': column_info['type'],
        'description': column_info.get('description', ''),
        'tests': column_info.get('tests', []),
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
    if column_details['column_type'] == 'source':
        return "None (Source Column)"

    prompt = f"""
    Analyze the following column and determine its likely upstream column(s) based on the information provided:

    Model: {column_details['model']}
    Column: {column_details['column']}
    Column Type: {column_details['column_type']}
    Description: {column_details['description']}
    Tests: {', '.join(column_details['tests'])}
    Upstream Models and their columns:
    {json.dumps(column_details['upstream_models'], indent=2)}

    Please identify the most likely upstream column(s) that were used to create this column.
    Explain your reasoning briefly. If you can't determine a direct lineage, provide your best guess
    based on naming conventions and typical data transformations.

    Response Format:
    Upstream Column(s): [list the likely upstream column(s)]
    Reasoning: [brief explanation]
    """

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(60))
    def retryable_query_gemini():
        return model.generate_content(prompt)

    response = retryable_query_gemini()
    return response.text

def analyze_lineage(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    results = []
    for model_name, model_data in data.items():
        for column_name, column_info in model_data['columns'].items():
            column_details = get_column_details(model_data, column_name)

            @retry(stop=stop_after_attempt(5), wait=wait_fixed(60))
            def query_gemini_with_retry(column_details):
                return query_gemini(column_details)

            try:
                gemini_response = query_gemini_with_retry(column_details)
                column_details['gemini_analysis'] = gemini_response
                results.append(column_details)
                print(f"Analyzed {model_name}.{column_name}")
            except Exception as e:
                print(f"Error analyzing column {column_name} in model {model_name}: {e}")
                print(f"Gemini query input: {column_details}")

    return results

def main():
    data = load_json_data('dbt_lineage_jaffle_shop.json')
    results = analyze_lineage(data)

    with open('lineage_analysis_results.json', 'w') as f:
        json.dump(results, f, indent=2)

    # Convert results to CSV
    with open('lineage_analysis_results.csv', 'w', newline='') as csvfile:
        fieldnames = ['model', 'column', 'column_type', 'description', 'tests', 'upstream_models', 'gemini_analysis']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            row = {
                'model': result['model'],
                'column': result['column'],
                'column_type': result['column_type'],
                'description': result['description'],
                'tests': ', '.join(result['tests']),
                'upstream_models': json.dumps(result['upstream_models']),
                'gemini_analysis': result['gemini_analysis']
            }
            writer.writerow(row)

    print("Analysis complete. Results saved to lineage_analysis_results.json and lineage_analysis_results.csv")

if __name__ == "__main__":
    main()