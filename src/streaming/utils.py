import os

def read_delta_table(spark, path: str):
    """
    Read a Delta table from the given path.
    """
    try:
        return spark.read.format("delta").load(str(path))
    except Exception as e:
        print(f"Error reading Delta table on path {path}: {e}")
        raise e

def write_delta_table(df, path: str, mode: str = "append"):
    """
    Write a DataFrame to a Delta table with the given mode.
    """
    try:
        writer = df.write.format("delta").mode(mode)
        if mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        elif mode == "append":
            writer = writer.option("mergeSchema", "true")
        writer.save(str(path))

        print(f"✅ Data successfully written to {path} (Mode: {mode})")
    except Exception as e:
        print(f"Error writing Delta table on path {path}: {e}")
        raise e

def get_clickhouse_client():
    """
    Responsible for connecting with the ClickHouse database.
    """
    import clickhouse_connect
    try:
        client = clickhouse_connect.get_client(
            host = os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            port = os.environ.get("CLICKHOUSE_PORT", "8123"),
            username = os.environ.get("CLICKHOUSE_USER", "default"),
            password = os.environ.get("CLICKHOUSE_PASSWORD", ""),
            database = os.environ.get("CLICKHOUSE_DB", "stock_market")
        )

        return client
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        raise e
        