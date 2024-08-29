import json
import csv
import os

def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def extract_lineage(manifest, catalog):
    lineage = {}
    project_name = manifest.get('metadata', {}).get('project_name', 'Unknown Project')
    
    # Process sources first
    for source_id, source in manifest['sources'].items():
        source_name = f"{source['source_name']}.{source['name']}"
        lineage[source_id] = {
            'project': project_name,
            'name': source_name,
            'type': 'source',
            'columns': {},
            'upstream_models': [],
            'database': source['database'],
            'schema': source['schema'],
            'materialized': 'source'  # Sources are always materialized
        }
        
        # Get column info for sources from catalog
        if source_id in catalog['sources']:
            for col_name, col_info in catalog['sources'][source_id]['columns'].items():
                lineage[source_id]['columns'][col_name] = {
                    'type': col_info['type'],
                    'description': source['columns'].get(col_name, {}).get('description', '')
                }

    # Process models
    for node_id, node in manifest['nodes'].items():
        if node['resource_type'] == 'model':
            model_name = node['name']
            lineage[node_id] = {
                'project': project_name,
                'name': model_name,
                'type': 'model',
                'columns': {},
                'upstream_models': [],
                'sql': node.get('raw_code', ''),
                'database': node['database'],
                'schema': node['schema'],
                'materialized': node['config']['materialized']
            }
            
            # Get upstream models and sources
            for upstream_node_id in manifest['parent_map'].get(node_id, []):
                upstream_node = manifest['nodes'].get(upstream_node_id) or manifest['sources'].get(upstream_node_id)
                if upstream_node:
                    if upstream_node['resource_type'] == 'model':
                        upstream_name = upstream_node['name']
                    elif upstream_node['resource_type'] == 'source':
                        upstream_name = f"{upstream_node['source_name']}.{upstream_node['name']}"
                    else:
                        continue  # Skip if not a model or source
                    
                    upstream_info = {
                        'id': upstream_node_id,
                        'name': upstream_name,
                        'type': upstream_node['resource_type'],
                        'columns': []
                    }
                    
                    # Get columns from upstream model or source
                    if upstream_node_id in catalog['nodes']:
                        upstream_columns = catalog['nodes'][upstream_node_id]['columns'].keys()
                    elif upstream_node_id in catalog['sources']:
                        upstream_columns = catalog['sources'][upstream_node_id]['columns'].keys()
                    else:
                        upstream_columns = []
                    
                    upstream_info['columns'] = list(upstream_columns)
                    lineage[node_id]['upstream_models'].append(upstream_info)
            
            # Get column info for models from catalog
            if node_id in catalog['nodes']:
                for col_name, col_info in catalog['nodes'][node_id]['columns'].items():
                    lineage[node_id]['columns'][col_name] = {
                        'type': col_info['type'],
                        'description': node['columns'].get(col_name, {}).get('description', '')
                    }

    return lineage

def lineage_to_csv(lineage, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['project', 'id', 'name', 'type', 'database', 'schema', 'materialized', 'column_name', 'column_type', 'column_description', 'sql', 'upstream_models']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for node_id, info in lineage.items():
            upstream_models = json.dumps([{
                'id': upstream['id'],
                'name': upstream['name'],
                'type': upstream['type'],
                'columns': upstream['columns']
            } for upstream in info['upstream_models']])
            
            for col_name, col_info in info['columns'].items():
                writer.writerow({
                    'project': info['project'],
                    'id': node_id,
                    'name': info['name'],
                    'type': info['type'],
                    'database': info['database'],
                    'schema': info['schema'],
                    'materialized': info['materialized'],
                    'column_name': col_name,
                    'column_type': col_info['type'],
                    'column_description': col_info['description'],
                    'sql': info.get('sql', ''),  # Sources don't have SQL
                    'upstream_models': upstream_models
                })

def lineage_to_json(lineage, output_file):
    with open(output_file, 'w', encoding='utf-8') as jsonfile:
        json.dump(lineage, jsonfile, indent=2)

def process_project(manifest_path, catalog_path):
    manifest = load_json(manifest_path)
    catalog = load_json(catalog_path)
    lineage = extract_lineage(manifest, catalog)
    
    project_name = manifest.get('metadata', {}).get('project_name', 'unknown_project')
    output_prefix = f"dbt_lineage_{project_name}"
    
    lineage_to_csv(lineage, f'{output_prefix}.csv')
    lineage_to_json(lineage, f'{output_prefix}.json')
    print(f"Lineage data has been written to {output_prefix}.csv and {output_prefix}.json")

def main():
    projects = [
        {
            'manifest': 'manifest.json',
            'catalog': 'catalog.json'
        },
        
        # Add more projects as needed in different folders 
    ]
    
    for project in projects:
        process_project(project['manifest'], project['catalog'])

if __name__ == "__main__":
    main()