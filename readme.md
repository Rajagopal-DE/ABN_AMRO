# ABN AMRO Case Study 

A config-driven ETL framework built on Azure Databricks, Delta Lake, and Unity Catalog. Adding a new pipeline requires only a new YAML config — no framework code changes needed.

---

## Architecture

architecture_diagram.png present in the folder

**Data flow:** ADLS CSV files → Bronze (raw) → DQ checks → Silver (cleaned) → Gold (consolidated)

---

## Repo Structure

```
ABN_AMRO/
    configs/
        global_config.yaml          # environment settings - only file to change per environment
        pipeline_clients.yaml
        pipeline_credits.yaml
        pipeline_collaterals.yaml
        pipeline_market.yaml
        pipeline_gold.yaml
    framework/
        etl_utils.py                # read, transform, write
        dq_checker.py               # DQ not_null check + audit logging - Sample
        executor.py                 # orchestrates pipeline steps
    notebooks/
        run_all_pipelines.py        # run all pipelines throught the notebook
    templates/
        pipeline_template.j2        # Jinja2 template for new pipelines
        generate_pipeline.py        # generates new pipeline YAML from template
    tests/
        test_etl.py                 # unit tests
        run_tests.py                # runs tests through notebooks
```

---

## Setup

**1. Clone the repo in Databricks:**
```
Workspace -> Repos -> Add Repo -> paste GitHub URL
```

**2. Upload CSV files to ADLS:**
```
srcdata container:
    clients.csv
    credits.csv
    collaterals.csv
    market.csv
```

**3. Update `global_config.yaml` with your environment:**

catalog:      your_catalog
source_base:  "abfss://srcdata@YOUR_STORAGE.dfs.core.windows.net"
managed_base: "abfss://your_container@YOUR_STORAGE.dfs.core.windows.net"
```

**4. Run the notebook:**

notebooks/run_all_pipelines.py -> For data load

---

## Output

You can check the tables in bronze, silver and gold.

---

## Adding a New Pipeline

1. Open templates/generate_pipeline.py
2. Fill in pipeline details, instruction and examples given
3. Run the file -> new YAML generated in configs/
4. Add run_pipeline("pipeline_xxx.yaml") in the notebook

No framework code changes needed.

---

## Unit Tests - run tests/run_tests.py
Sample Unit test below can be executed:
    - Config validation
    - Uppercase transformation
    - DQ not_null check

---

