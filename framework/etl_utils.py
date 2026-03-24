import yaml
import os
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pyspark.sql.functions as F

CONFIG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../configs")


# config

def load_config(filename):
    with open(os.path.join(CONFIG_DIR, "global_config.yaml")) as f:
        global_cfg = yaml.safe_load(f)

    with open(os.path.join(CONFIG_DIR, filename)) as f:
        pipeline_cfg = yaml.safe_load(f)

    config = {**global_cfg, **pipeline_cfg}

    if "pipeline_name" not in config:
        raise ValueError("pipeline_name is missing in config")
    if "source" in config and "path" not in config["source"]:
        raise ValueError("source.path is missing in config")

    
    _resolve_placeholders(config)

    return config


def _resolve_placeholders(config):
    catalog     = config["catalog"]
    source_base = config["source_base"]

    
    if "source" in config and "path" in config["source"]:
        config["source"]["path"] = config["source"]["path"].replace(
            "{source_base}", source_base
        )

   
    for t in config.get("transformations", []):
        if "join_table" in t:
            t["join_table"] = t["join_table"].replace("{catalog}", catalog)

    
    if "gold_sql" in config:
        config["gold_sql"] = config["gold_sql"].replace("{catalog}", catalog)


# reader

def read_source(spark, config):
    src  = config["source"]
    path = src["path"]

    print(f"Reading: {path}")

    df = (spark.read
          .format("csv")
          .option("header", str(src.get("has_header", True)).lower())
          .option("inferSchema", "false")
          .load(path))

    cols = src.get("columns", [])
    if cols:
        df = df.select(cols)

    df = df.dropDuplicates()

    count = df.count()
    print(f"Rows read: {count}")
    return df, count


# transformations

def apply_transformations(spark, df, config):
    transforms = config.get("transformations", [])

    if not transforms:
        print("No transformations defined")
        return df

    for t in transforms:
        action = t["action"]
        col    = t.get("column")

        if action == "uppercase":
            print(f"  uppercase -> {col}")
            df = df.withColumn(col, F.upper(F.col(col)))

        elif action == "stock_enrichment":
            print(f"  stock_enrichment -> {col}")
            df = _enrich_stock_valuation(spark, df, t)

        else:
            raise ValueError(f"Unknown transformation action: {action}")

    return df


def _enrich_stock_valuation(spark, df, t):
    market_df = spark.table(t["join_table"])

    df = df.alias("collaterals").join(
        market_df.alias("market"),
        F.expr(t["join_on"]),
        how="left"
    )

    df = df.withColumn("numbers",           F.col("numbers").cast("double"))
    df = df.withColumn("current_value_eur", F.col("current_value_eur").cast("double"))
    df = df.withColumn("valuation_eur",     F.col("valuation_eur").cast("double"))

    df = df.withColumn(t["column"], F.expr(t["expression"]))

    cols_to_drop = [c for c in market_df.columns if c != t["column"]]
    df = df.drop(*cols_to_drop)

    return df


# writers

def write_bronze(spark, df, config):
    df    = _add_audit_cols(df, config["pipeline_name"], "bronze")
    table = _full_table(config, "bronze")
    path  = _path(config, "bronze")
    _ensure_db(spark, config, "bronze")
    _overwrite(df, table, path)
    count = spark.table(table).count()
    print(f"Bronze: {count} rows -> {table}")
    return count


def write_silver(spark, df, config):
    df        = _add_audit_cols(df, config["pipeline_name"], "silver")
    table     = _full_table(config, "silver")
    path      = _path(config, "silver")
    merge_key = config["targets"]["silver"].get("merge_key")
    _ensure_db(spark, config, "silver")

    if not spark.catalog.tableExists(table):
        _overwrite(df, table, path)
    else:
        _merge(spark, df, table, merge_key)

    count = spark.table(table).count()
    print(f"Silver: {count} rows -> {table}")
    return count


def write_gold(spark, config):
    if "gold_sql" not in config or "gold" not in config.get("targets", {}):
        return 0

    table = _full_table(config, "gold")
    path  = _path(config, "gold")
    _ensure_db(spark, config, "gold")

    df = spark.sql(config["gold_sql"])
    df = _add_audit_cols(df, config["pipeline_name"], "gold")
    _overwrite(df, table, path)

    count = spark.table(table).count()
    print(f"Gold: {count} rows -> {table}")
    return count


# audit logger

def log_run(spark, config, meta):
    cat      = config["catalog"]
    db       = config["audit_database"]
    tbl      = config["audit_table"]
    full_tbl = f"{cat}.{db}.{tbl}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cat}.{db}")

    duration = None
    if meta.get("start_time") and meta.get("end_time"):
        duration = (meta["end_time"] - meta["start_time"]).total_seconds()

    schema = StructType([
        StructField("pipeline_name",    StringType(),  True),
        StructField("status",           StringType(),  True),
        StructField("start_time",       StringType(),  True),
        StructField("end_time",         StringType(),  True),
        StructField("duration_seconds", DoubleType(),  True),
        StructField("source_count",     IntegerType(), True),
        StructField("bronze_count",     IntegerType(), True),
        StructField("silver_count",     IntegerType(), True),
        StructField("gold_count",       IntegerType(), True),
        StructField("dq_passed",        IntegerType(), True),
        StructField("dq_failed",        IntegerType(), True),
        StructField("error_message",    StringType(),  True),
    ])

    data = [(
        config["pipeline_name"],
        meta.get("status", "UNKNOWN"),
        str(meta.get("start_time", "")),
        str(meta.get("end_time", "")),
        float(duration) if duration else None,
        int(meta.get("source_count", 0)),
        int(meta.get("bronze_count", 0)),
        int(meta.get("silver_count", 0)),
        int(meta.get("gold_count", 0)),
        int(meta.get("dq_passed", 0)),
        int(meta.get("dq_failed", 0)),
        meta.get("error_message", None),
    )]

    (spark.createDataFrame(data, schema=schema)
          .write
          .format("delta")
          .mode("append")
          .option("mergeSchema", "true")
          .saveAsTable(full_tbl))

    print(f"Audit log -> {full_tbl} [{meta.get('status')}]")


# helpers

def _full_table(config, layer):
    t = config["targets"][layer]
    return f"{config['catalog']}.{t['database']}.{t['table']}"


def _path(config, layer):
    t = config["targets"][layer]
    return f"{config['managed_base']}/{layer}/{t['table']}"


def _ensure_db(spark, config, layer):
    cat = config["catalog"]
    db  = config["targets"][layer]["database"]
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {cat}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cat}.{db}")


def _overwrite(df, table, path):
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .option("path", path)
       .saveAsTable(table))


def _merge(spark, df, table, merge_key):
    dt = DeltaTable.forName(spark, table)
    (dt.alias("t")
       .merge(df.alias("s"), f"t.{merge_key} = s.{merge_key}")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute())


def _add_audit_cols(df, pipeline_name, layer):
    return (df
            .withColumn("_pipeline",  F.lit(pipeline_name))
            .withColumn("_layer",     F.lit(layer))
            .withColumn("_loaded_at", F.current_timestamp()))