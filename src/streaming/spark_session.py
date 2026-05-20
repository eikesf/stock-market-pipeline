import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def create_spark_session():
    """
    Create a Spark Session with Delta Lake configuration.
    """
    spark = None
    try:
        builder = SparkSession.builder \
            .appName("Stock Market Pipeline") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )

        # Temporarily redirect stderr to suppress Ivy/Java startup logs
        stderr_fileno = sys.stderr.fileno()
        with os.fdopen(os.dup(stderr_fileno), 'wb') as backup:
            with open(os.devnull, 'wb') as devnull:
                os.dup2(devnull.fileno(), stderr_fileno)
                
                try:
                    # Initialize Spark Session (where Ivy and Java warnings are emitted)
                    spark = configure_spark_with_delta_pip(builder).getOrCreate()
                    spark.sparkContext.setLogLevel("ERROR")
                finally:
                    # Always restore stderr
                    os.dup2(backup.fileno(), stderr_fileno)

    except Exception as e:
        print(f"Error creating SparkSession: {e}")
        exit(1)
        
    print("Spark Session created successfully.")
    return spark
