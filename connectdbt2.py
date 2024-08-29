import json
import csv

def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def extract_lineage(manifest, catalog):
    lineage = {}
    for node_id, node in manifest['nodes'].items():
        if node['resource_type'] == 'model':
            model_name = node['name']
            lineage[model_name] = {
                'columns': {},
                'upstream_models': {},
                'sql': node.get('raw_code', ''),
            }
            
            # Get upstream models and their columns
            for upstream_node_id in manifest['parent_map'].get(node_id, []):
                upstream_node = manifest['nodes'].get(upstream_node_id)
                if upstream_node and upstream_node['resource_type'] == 'model':
                    upstream_model_name = upstream_node['name']
                    lineage[model_name]['upstream_models'][upstream_model_name] = []
                    
                    # Get columns from upstream model
                    if upstream_node_id in catalog['nodes']:
                        upstream_columns = catalog['nodes'][upstream_node_id]['columns'].keys()
                        lineage[model_name]['upstream_models'][upstream_model_name] = list(upstream_columns)
            
            # Get column info from catalog
            if node_id in catalog['nodes']:
                for col_name, col_info in catalog['nodes'][node_id]['columns'].items():
                    lineage[model_name]['columns'][col_name] = {
                        'type': col_info['type'],
                        'description': node['columns'].get(col_name, {}).get('description', '')
                    }

    return lineage

def lineage_to_csv(lineage, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['model', 'column_name', 'column_type', 'column_description', 'sql', 'upstream_models_and_columns']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for model, info in lineage.items():
            upstream_models_and_columns = '; '.join([f"{model}: {', '.join(columns)}" for model, columns in info['upstream_models'].items()])
            sql = info['sql']
            
            for col_name, col_info in info['columns'].items():
                writer.writerow({
                    'model': model,
                    'column_name': col_name,
                    'column_type': col_info['type'],
                    'column_description': col_info['description'],
                    'sql': sql,
                    'upstream_models_and_columns': upstream_models_and_columns
                })

def lineage_to_json(lineage, output_file):
    with open(output_file, 'w', encoding='utf-8') as jsonfile:
        json.dump(lineage, jsonfile, indent=2)

def main():
    manifest = load_json('manifest.json')
    catalog = load_json('catalog.json')
    lineage = extract_lineage(manifest, catalog)
    lineage_to_csv(lineage, 'dbt_lineage.csv')
    lineage_to_json(lineage, 'dbt_lineage.json')
    print("Lineage data has been written to dbt_lineage.csv and dbt_lineage.json")

if __name__ == "__main__":
    main()