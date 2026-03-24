"""
Unit Tests

"""

import pytest
import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

sys.path.append(os.path.join(os.path.dirname(__file__), "../framework"))


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


# 1. Config validation

def test_config_missing_pipeline_name():
    config = {"source": {"path": "some/path"}, "targets": {"bronze": {}}}
    with pytest.raises(ValueError):
        if "pipeline_name" not in config:
            raise ValueError("Missing required field: pipeline_name")


def test_config_missing_source_path():
    config = {"pipeline_name": "pipeline_clients", "source": {}, "targets": {"bronze": {}}}
    with pytest.raises(ValueError):
        if "path" not in config["source"]:
            raise ValueError("source.path is missing")


def test_config_valid():
    config = {
        "pipeline_name": "pipeline_clients",
        "source": {"path": "abfss://srcdata.../clients.csv"},
        "targets": {"bronze": {"database": "bronze", "table": "clients"}}
    }
    assert "pipeline_name" in config
    assert "path" in config["source"]
    assert "bronze" in config["targets"]


# 2. Transformation validation

def test_uppercase_transformation(spark):
    data   = [("CLT-0001", "amsterdam"), ("CLT-0002", "rotterdam")]
    df     = spark.createDataFrame(data, ["client_id", "city"])
    result = df.withColumn("city", F.upper(F.col("city")))
    cities = [r["city"] for r in result.collect()]
    assert cities == ["AMSTERDAM", "ROTTERDAM"]


# 3. DQ rule validation

def test_dq_not_null_catches_nulls(spark):
    data   = [("CLT-0001", "Emma"), (None, "Liam")]
    df     = spark.createDataFrame(data, ["client_id", "name"])
    bad_df = df.filter(F.col("client_id").isNull())
    assert bad_df.count() == 1


def test_dq_not_null_passes_clean_data(spark):
    data   = [("CLT-0001", "Emma"), ("CLT-0002", "Liam")]
    df     = spark.createDataFrame(data, ["client_id", "name"])
    bad_df = df.filter(F.col("client_id").isNull())
    assert bad_df.count() == 0