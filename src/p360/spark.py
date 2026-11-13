"""One local Spark session with Delta Lake, sized for a 16 GB laptop.

Resource plan (measured in results/run_summary.json): 4 cores, 5 GB driver heap, 64 shuffle partitions
(the biggest table is ~1.5 GB as Parquet, so 64 partitions is ~25 MB each), off-heap Parquet reads,
nothing is cached or collected unless it is small. Override with P360_SPARK_* env vars.
"""

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

_session: SparkSession | None = None


def get_spark() -> SparkSession:
    global _session
    if _session is not None:
        return _session
    os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@17")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (
        SparkSession.builder.appName("provider-360")
        .master(os.environ.get("P360_SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.environ.get("P360_SPARK_DRIVER_MEMORY", "5g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("P360_SPARK_SHUFFLE_PARTITIONS", "64"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "zstd")  # ~30% smaller than snappy on this data
        .config("spark.sql.files.maxPartitionBytes", "32m")  # more, smaller input splits for 4 cores
        # every new Delta table records a change data feed: gold reads silver/gold changes incrementally
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/p360-derby")
    )
    _session = configure_spark_with_delta_pip(builder).getOrCreate()
    _session.sparkContext.setLogLevel("ERROR")
    return _session


def stop_spark() -> None:
    global _session
    if _session is not None:
        _session.stop()
        _session = None
