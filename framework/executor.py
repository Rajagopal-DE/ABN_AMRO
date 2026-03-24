import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession

FRAMEWORK_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(FRAMEWORK_DIR)

from etl_utils  import load_config, read_source, apply_transformations
from etl_utils  import write_bronze, write_silver, write_gold, log_run
from dq_checker import run_dq_checks


def run_pipeline(config_file):
    """
    Runs a full ETL pipeline from CSV to Silver.
    Call run_gold() separately after all 4 pipelines are done.

    Usage:
        run_pipeline("pipeline_clients.yaml")
        run_pipeline("pipeline_credits.yaml")
        run_pipeline("pipeline_collaterals.yaml")
        run_pipeline("pipeline_market.yaml")
    """
    spark = SparkSession.builder.getOrCreate()
    start = datetime.utcnow()
    meta  = {"start_time": start}

    print(f"\n{'='*50}")
    print(f" Starting: {config_file}")
    print(f"{'='*50}\n")

    try:
        # 1. load config
        config = load_config(config_file)
        print(f"Pipeline: {config['pipeline_name']}\n")

        # 2. read CSV from ADLS
        df_raw, source_count = read_source(spark, config)
        meta["source_count"] = source_count

        # 3. write Bronze - raw copy
        print("\n-- Bronze --")
        meta["bronze_count"] = write_bronze(spark, df_raw, config)

        # 4. run DQ checks 
        print("\n-- DQ Checks --")
        df_clean, dq_results  = run_dq_checks(spark, df_raw, config)
        total_rules           = len(config.get("data_quality", []))
        meta["dq_failed"]     = len(dq_results)
        meta["dq_passed"]     = total_rules - meta["dq_failed"]

        # 5. apply transformations
        print("\n-- Transformations --")
        df_transformed = apply_transformations(spark, df_clean, config)

        # 6. write Silver
        print("\n-- Silver --")
        meta["silver_count"] = write_silver(spark, df_transformed, config)

        meta["status"]   = "SUCCESS"
        meta["end_time"] = datetime.utcnow()

        print(f"\n{'='*50}")
        print(f" Done: {config['pipeline_name']}")
        print(f" source={source_count} bronze={meta['bronze_count']} silver={meta['silver_count']}")
        print(f" DQ passed={meta['dq_passed']} failed={meta['dq_failed']}")
        print(f"{'='*50}\n")

    except Exception as e:
        meta["status"]        = "FAILED"
        meta["end_time"]      = datetime.utcnow()
        meta["error_message"] = str(e)
        print(f"\n Pipeline failed: {e}")
        try:
            log_run(spark, config, meta)
        except Exception:
            pass
        raise

    log_run(spark, config, meta)


def run_gold(config_file="pipeline_gold.yaml"):
    """
    Builds the Gold layer after all 4 Silver tables are ready.
    Joins all silver tables and produces credit_risk_fact.

    Usage:
        run_gold()
    """
    spark = SparkSession.builder.getOrCreate()
    start = datetime.utcnow()
    meta  = {"start_time": start}

    print(f"\n{'='*50}")
    print(f" Building Gold layer")
    print(f"{'='*50}\n")

    try:
    
        config = load_config(config_file)

        print("\n-- Gold --")
        meta["gold_count"] = write_gold(spark, config)

        meta["status"]   = "SUCCESS"
        meta["end_time"] = datetime.utcnow()

        print(f"\n{'='*50}")
        print(f" Gold complete: {meta['gold_count']} rows -> credit_risk_fact")
        print(f"{'='*50}\n")

    except Exception as e:
        meta["status"]        = "FAILED"
        meta["end_time"]      = datetime.utcnow()
        meta["error_message"] = str(e)
        print(f"\n Gold failed: {e}")
        try:
            log_run(spark, config, meta)
        except Exception:
            pass
        raise

    log_run(spark, config, meta)