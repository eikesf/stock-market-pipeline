import logging
import os
import shutil
import tempfile
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Set a temporary DATA_DIR for all tests to isolate them from real workspace data
TEST_DATA_DIR = tempfile.mkdtemp()
os.environ["DATA_DIR"] = TEST_DATA_DIR

# Suppress py4j info/warning logs during teardown to avoid ValueError: I/O operation on closed file
logging.getLogger("py4j").setLevel(logging.ERROR)


def pytest_sessionfinish(session, exitstatus):
    """Clean up the temporary data directory after all tests finish."""
    if Path(TEST_DATA_DIR).exists():
        shutil.rmtree(TEST_DATA_DIR)


@pytest.fixture(scope="session")
def spark_session():
    """
    Create a local, reusable Spark Session with Delta support for testing.
    """

    temp_dir = tempfile.mkdtemp()
    warehouse_dir = Path(temp_dir) / "spark-warehouse"
    metastore_dir = Path(temp_dir) / "metastore_db"

    builder = (
        SparkSession.builder.appName("Pipeline Tests")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={metastore_dir}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()

    # Force garbage collection to clean up Py4J objects before logging handlers are closed
    import gc

    gc.collect()

    if Path(temp_dir).exists():
        shutil.rmtree(temp_dir)
