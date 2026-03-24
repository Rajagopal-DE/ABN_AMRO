from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F


def run_dq_checks(spark, df, config):
    """
    Runs not_null DQ check on primary key after Bronze, before Silver.
    
    """
    rules       = config.get("data_quality", [])
    primary_key = config.get("primary_key")
    identify_by = config.get("identify_by")
    results     = []

    if not rules:
        print("No DQ rules - skipping")
        return df, results

    print(f"Running DQ checks for: {config['pipeline_name']}")

    for rule in rules:
        action    = rule.get("action", "drop_row")
        col       = rule.get("column")
        rule_name = f"{col}_not_null"

        try:
            bad_df = df.filter(F.col(col).isNull())
            count  = bad_df.count()
        except Exception as e:
            print(f"  WARNING {rule_name}: ERROR - {e}")
            continue

        if count == 0:
            _log(spark, config, rule_name, "PASS", 0, None, action)
            print(f"  PASS {rule_name}")
            continue

        failed_ids = _get_failed_ids(bad_df, identify_by)
        _log(spark, config, rule_name, "FAIL", count, failed_ids, action)
        print(f"  FAIL {rule_name} - {count} rows - ids: {failed_ids}")
        results.append({"rule": rule_name, "status": "FAIL", "failed_rows": count})

        if action == "drop_row":
            df = df.filter(F.col(col).isNotNull())
            print(f"     {count} rows dropped")

    passed = len(rules) - len(results)
    print(f"DQ done: {passed} passed, {len(results)} failed\n")
    return df, results


def _get_failed_ids(bad_df, identify_by):
    try:
        if identify_by and identify_by in bad_df.columns:
            rows   = bad_df.select(identify_by).limit(5).collect()
            ids    = [f"{identify_by}={r[identify_by]}" for r in rows]
        else:
            ids = ["no identifying column available"]

        total  = bad_df.count()
        result = ", ".join(ids)
        if total > 5:
            result += f" ... and {total - 5} more"
        return result

    except Exception:
        return "could not retrieve IDs"


def _log(spark, config, rule_name, status, failed_count, failed_ids, action):
    cat      = config["catalog"]
    db       = config["audit_database"]
    tbl      = config["dq_table"]
    full_tbl = f"{cat}.{db}.{tbl}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cat}.{db}")

    schema = StructType([
        StructField("pipeline_name", StringType(),  True),
        StructField("rule",          StringType(),  True),
        StructField("status",        StringType(),  True),
        StructField("failed_rows",   IntegerType(), True),
        StructField("failed_ids",    StringType(),  True),
        StructField("action",        StringType(),  True),
        StructField("checked_at",    StringType(),  True),
    ])

    data = [(
        str(config["pipeline_name"]),
        str(rule_name),
        str(status),
        int(failed_count),
        str(failed_ids) if failed_ids is not None else None,
        str(action),
        str(datetime.utcnow()),
    )]

    (spark.createDataFrame(data, schema=schema)
          .write
          .format("delta")
          .mode("append")
          .option("mergeSchema", "true")
          .saveAsTable(full_tbl))