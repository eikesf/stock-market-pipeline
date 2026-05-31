import pytest
import os
import shutil
import tempfile
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

@pytest.fixture(scope="session")
def spark_session():
    """
    Create a local, reusable Spark Session with Delta support for testing.
    """
    # Create temp directories for warehouse and derby metastore to keep test execution clean
    temp_dir = tempfile.mkdtemp()
    warehouse_dir = os.path.join(temp_dir, "spark-warehouse")
    metastore_dir = os.path.join(temp_dir, "metastore_db")
    
    builder = SparkSession.builder \
        .appName("Pipeline Tests") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={metastore_dir}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.enabled", "false")
        
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    # Tear down session and clean up temp files
    spark.stop()
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
