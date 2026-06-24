import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.utils.logger import logger


def create_spark_session() -> SparkSession:
    """Create a Spark Session with Delta Lake configuration.

    Configures local filesystems, Delta extensions, catalogs, suppresses noisy
    Spark logs during initialization, and returns the session instance.

    Returns:
        The active SparkSession instance.

    Raises:
        SystemExit: If Spark Session initialization fails.
    """
    logger.info("Initializing Spark Session...")
    spark = None
    try:
        builder = (
            SparkSession.builder.appName("Stock Market Pipeline")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        )

        # Checking stderr stream for pytest/notebooks compatibility.
        has_fileno = False
        try:
            stderr_fileno = sys.stderr.fileno()
            has_fileno = True
        except Exception:
            has_fileno = False

        if has_fileno:
            with os.fdopen(os.dup(stderr_fileno), "wb") as backup, open(os.devnull, "wb") as devnull:
                os.dup2(devnull.fileno(), stderr_fileno)

                try:
                    spark = configure_spark_with_delta_pip(builder).getOrCreate()
                    spark.sparkContext.setLogLevel("ERROR")
                finally:
                    os.dup2(backup.fileno(), stderr_fileno)
        else:
            logger.debug("Stderr redirection skipped (unsupported file descriptor in this environment).")
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")

    except Exception as e:
        logger.exception(f"Error creating SparkSession: {e}")
        sys.exit(1)

    logger.success("Spark Session created successfully.")
    return spark
