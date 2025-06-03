import os
import yaml
from jinja2 import Environment, FileSystemLoader

def generate_sql(manifest_path, template_path, output_path):
    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Load the YAML manifest
    with open(manifest_path, 'r') as file:
        data_model = yaml.safe_load(file)

    # Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader(template_path))
    template = env.get_template('bron_view.sql.j2')

    # Render the template with the data model
    sql = template.render(data_model)

    # Write the generated SQL to a file
    with open(output_path, 'w') as file:
        file.write(sql)

    print(f"SQL generated and saved to {output_path}")

# Example usage
if __name__ == "__main__":
    generate_sql(
        manifest_path='manifests/base/bron_syntess.yaml',
        template_path='sql_templates',
        output_path='output/bron_table.sql'
    )