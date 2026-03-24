"""
Use this script to generate a new pipeline YAML for any new dataset.

Steps:
    1. Fill in your values in the pipeline dict below
    2. Run this file in Databricks
    3. New pipeline YAML will be created in configs/ folder

Field descriptions: Example
    pipeline_name : name for the pipeline e.g. "pipeline_location"
    source_file   : CSV file name in ADLS e.g. "location.csv"
    table_name    : Delta table name e.g. "location"
    primary_key   : unique ID column e.g. "loc_id"
    identify_by   : column shown in DQ log when primary key is null e.g. "city"
    columns       : list of column names exactly as they appear in the CSV
    uppercase     : list of text columns to standardise to uppercase in silver
                    leave empty [] if not needed
    dq_rules      : list of not_null checks — leave empty [] if not needed

DQ rule format:
    {"column": "col_name", "action": "drop_row"}
"""

import os
import yaml
from jinja2 import Environment, FileSystemLoader

# Paths from current notebook location using dbutils
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root     = "/Workspace" + "/".join(notebook_path.split("/")[:-2])
TEMPLATE_DIR  = repo_root + "/templates"
CONFIG_DIR    = repo_root + "/configs"
OUTPUT_DIR    = CONFIG_DIR

# source_base 
with open(os.path.join(CONFIG_DIR, "global_config.yaml")) as f:
    global_cfg = yaml.safe_load(f)

source_base = global_cfg["source_base"]

env      = Environment(loader=FileSystemLoader(TEMPLATE_DIR), trim_blocks=True, lstrip_blocks=True)
template = env.get_template("pipeline_template.j2")

# Fill in your pipeline details here - example given below

pipeline = {
    "pipeline_name": "pipeline_location",
    "source_file":   "location.csv",
    "table_name":    "location",
    "primary_key":   "loc_id",
    "identify_by":   "city",
    "source_base":   source_base,
    "columns": [
        "loc_id",
        "city",
        "country",
        "zip_code",
    ],
    "uppercase": ["city", "country"],
    "dq_rules": [
        {"column": "loc_id", "action": "drop_row"},
    ]
}

# Generate YAML 

rendered    = template.render(**pipeline)
output_file = os.path.join(OUTPUT_DIR, f"{pipeline['pipeline_name']}.yaml")

with open(output_file, "w") as f:
    f.write(rendered)

print(f"Generated: {pipeline['pipeline_name']}.yaml in configs/")