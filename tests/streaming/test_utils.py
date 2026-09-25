import json
import os
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from src.streaming.utils import (
    check_and_heal_corrupt_data_file,
    extract_corrupt_parquet_filename,
    find_archive_dates,
    find_version_introducing_file,
    get_clickhouse_client,
    heal_corrupt_delta_checkpoints,
    is_corruption_error,
    read_delta_table,
    recover_bronze_from_archive,
    replay_archive_to_bronze,
    verify_delta_table,
    write_delta_table,
)


@patch("clickhouse_connect.get_client")
def test_get_clickhouse_client(mock_get_client):
    """
    Test that the ClickHouse client is initialized with configurations from environment variables.
    """
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    env_vars = {
        "CLICKHOUSE_HOST": "localhost",
        "CLICKHOUSE_PORT": "9000",
        "CLICKHOUSE_USER": "test_user",
        "CLICKHOUSE_PASSWORD": "test_password",
        "CLICKHOUSE_DB": "test_db",
    }

    with patch.dict(os.environ, env_vars):
        client = get_clickhouse_client()

    assert client == mock_client
    mock_get_client.assert_called_once_with(
        host="localhost", port="9000", username="test_user", password="test_password", database="test_db"
    )


def test_read_delta_table():
    """
    Test that reading a Delta table invokes Spark's read API with the correct format and path.
    """
    mock_spark = MagicMock()
    mock_df = MagicMock()

    mock_spark.read.format.return_value.load.return_value = mock_df

    res = read_delta_table(mock_spark, "dummy_path")

    assert res == mock_df
    mock_spark.read.format.assert_called_once_with("delta")
    mock_spark.read.format.return_value.load.assert_called_once_with("dummy_path")


def test_write_delta_table_append():
    """
    Test that writing a Delta table in append mode configures the mergeSchema option.
    """
    mock_df = MagicMock()
    mock_writer = MagicMock()

    mock_df.write.format.return_value.mode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer

    write_delta_table(mock_df, "dummy_path", mode="append")

    mock_df.write.format.assert_called_once_with("delta")
    mock_df.write.format.return_value.mode.assert_called_once_with("append")
    mock_writer.option.assert_called_once_with("mergeSchema", "true")
    mock_writer.save.assert_called_once_with("dummy_path")


def test_write_delta_table_overwrite():
    """
    Test that writing a Delta table in overwrite mode configures the overwriteSchema option.
    """
    mock_df = MagicMock()
    mock_writer = MagicMock()

    mock_df.write.format.return_value.mode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer

    write_delta_table(mock_df, "dummy_path", mode="overwrite")

    mock_df.write.format.assert_called_once_with("delta")
    mock_df.write.format.return_value.mode.assert_called_once_with("overwrite")
    mock_writer.option.assert_called_once_with("overwriteSchema", "true")
    mock_writer.save.assert_called_once_with("dummy_path")


def test_heal_corrupt_delta_checkpoints_no_corruption(tmp_path):
    """Test that a healthy checkpoint is not modified."""
    log_dir = tmp_path / "_delta_log"
    log_dir.mkdir()

    # Write a valid checkpoint file
    checkpoint_file = log_dir / "00000000000000000100.checkpoint.parquet"
    table = pa.table({"col": [1, 2, 3]})
    pq.write_table(table, str(checkpoint_file))

    # Write _last_checkpoint
    last_checkpoint_file = log_dir / "_last_checkpoint"
    checkpoint_info = {
        "version": 100,
        "size": 12,
        "sizeInBytes": checkpoint_file.stat().st_size,
        "numOfAddFiles": 1,
    }
    with open(last_checkpoint_file, "w") as f:
        json.dump(checkpoint_info, f)

    # Run healing
    heal_corrupt_delta_checkpoints(tmp_path)

    # Assert nothing was deleted
    assert checkpoint_file.exists()
    assert last_checkpoint_file.exists()
    with open(last_checkpoint_file) as f:
        data = json.load(f)
    assert data["version"] == 100


def test_heal_corrupt_delta_checkpoints_heals_to_previous(tmp_path):
    """Test healing when current checkpoint is corrupt but a previous valid one exists."""
    log_dir = tmp_path / "_delta_log"
    log_dir.mkdir()

    # Write a valid v100 checkpoint file
    prev_checkpoint = log_dir / "00000000000000000100.checkpoint.parquet"
    table = pa.table({"col": [1, 2, 3]})
    pq.write_table(table, str(prev_checkpoint))

    # Write a corrupted v110 checkpoint file
    corrupt_checkpoint = log_dir / "00000000000000000110.checkpoint.parquet"
    with open(corrupt_checkpoint, "wb") as f:
        f.write(b"corrupt header thrift data")

    # Write _last_checkpoint pointing to corrupt v110
    last_checkpoint_file = log_dir / "_last_checkpoint"
    checkpoint_info = {"version": 110, "size": 12, "sizeInBytes": 100, "numOfAddFiles": 1}
    with open(last_checkpoint_file, "w") as f:
        json.dump(checkpoint_info, f)

    # Run healing
    heal_corrupt_delta_checkpoints(tmp_path)

    # Assert corrupt file was deleted
    assert not corrupt_checkpoint.exists()
    # Assert previous valid file still exists
    assert prev_checkpoint.exists()
    # Assert _last_checkpoint was updated to v100
    with open(last_checkpoint_file) as f:
        data = json.load(f)
    assert data["version"] == 100


def test_heal_corrupt_delta_checkpoints_deletes_last_checkpoint_when_no_prev(tmp_path):
    """Test healing when current checkpoint is corrupt and no valid previous checkpoint exists."""
    log_dir = tmp_path / "_delta_log"
    log_dir.mkdir()

    # Write a corrupted v110 checkpoint file
    corrupt_checkpoint = log_dir / "00000000000000000110.checkpoint.parquet"
    with open(corrupt_checkpoint, "wb") as f:
        f.write(b"corrupt header thrift data")

    # Write _last_checkpoint pointing to corrupt v110
    last_checkpoint_file = log_dir / "_last_checkpoint"
    checkpoint_info = {"version": 110, "size": 12, "sizeInBytes": 100, "numOfAddFiles": 1}
    with open(last_checkpoint_file, "w") as f:
        json.dump(checkpoint_info, f)

    # Run healing
    heal_corrupt_delta_checkpoints(tmp_path)

    # Assert corrupt file was deleted
    assert not corrupt_checkpoint.exists()
    # Assert _last_checkpoint was deleted entirely to trigger log replay fallback
    assert not last_checkpoint_file.exists()


def test_extract_corrupt_parquet_filename():
    error_msg = "Encountered error while reading file file:/opt/airflow/data/bronze/prices/part-00000-e9315c03-ba78-44f0-8b12-2d7dfd694672-c000.snappy.parquet. SQLSTATE: KD001"
    fn = extract_corrupt_parquet_filename(error_msg)
    assert fn == "part-00000-e9315c03-ba78-44f0-8b12-2d7dfd694672-c000.snappy.parquet"

    # Test no match
    assert extract_corrupt_parquet_filename("some other error") is None


def test_find_version_introducing_file(tmp_path):
    log_dir = tmp_path / "_delta_log"
    log_dir.mkdir()

    # Create dummy commit files
    c126 = log_dir / "00000000000000000126.json"
    with open(c126, "w", encoding="utf-8") as f:
        f.write('{"add":{"path":"part-00000-abc.parquet","size":123}}\n')

    c127 = log_dir / "00000000000000000127.json"
    with open(c127, "w", encoding="utf-8") as f:
        f.write('{"add":{"path":"part-00000-corrupt.parquet","size":456}}\n')

    v = find_version_introducing_file(tmp_path, "part-00000-corrupt.parquet")
    assert v == 127

    v_missing = find_version_introducing_file(tmp_path, "missing.parquet")
    assert v_missing is None


def test_check_and_heal_corrupt_data_file(tmp_path):
    # Setup log dir and files
    log_dir = tmp_path / "_delta_log"
    log_dir.mkdir()

    corrupt_fn = "part-00000-corrupt.parquet"
    corrupt_file = tmp_path / corrupt_fn
    with open(corrupt_file, "w") as f:
        f.write("bad binary data")

    # Create .crc file too
    crc_file = tmp_path / f".{corrupt_fn}.crc"
    with open(crc_file, "w") as f:
        f.write("crc data")

    # Write commit JSON adding the file
    with open(log_dir / "00000000000000000127.json", "w", encoding="utf-8") as f:
        f.write(f'{{"add":{{"path":"{corrupt_fn}","size":456}}}}\n')

    # Mock Spark and DeltaTable
    mock_spark = MagicMock()
    mock_dt = MagicMock()

    error_msg = f"Encountered error reading file {corrupt_fn}"

    with patch("delta.tables.DeltaTable.forPath", return_value=mock_dt):
        healed = check_and_heal_corrupt_data_file([tmp_path], error_msg, mock_spark)

    # The healed table's path is returned, not just a flag: callers use it to tell a Bronze
    # rollback (which needs an archive replay) from a Silver one (which can be recomputed).
    assert healed == tmp_path
    # Assert it called restoreToVersion with prev version (126)
    mock_dt.restoreToVersion.assert_called_once_with(126)
    # Assert the physical files were deleted
    assert not corrupt_file.exists()
    assert not crc_file.exists()


def _write_delta_like_table(tmp_path, rows=3):
    """Create a directory shaped like a Delta table with one readable data file."""
    table = tmp_path / "table"
    (table / "_delta_log").mkdir(parents=True, exist_ok=True)
    data_file = table / "part-00000-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee-c000.snappy.parquet"
    pq.write_table(pa.table({"a": list(range(rows))}), str(data_file))
    return table, data_file


def test_verify_delta_table_reports_nothing_for_a_healthy_table(tmp_path):
    """Test that a table whose data files all read cleanly reports no corruption."""
    table, _ = _write_delta_like_table(tmp_path)

    assert verify_delta_table(table) == []


def test_verify_delta_table_detects_a_zero_filled_data_file(tmp_path):
    """Test detection of the observed corruption shape: zero-filled head, intact footer.

    This is what the Docker bind mount produced: the parquet footer survived while the earlier
    data pages were lost, so the file looks structurally plausible until a row group is read.
    """
    table, data_file = _write_delta_like_table(tmp_path, rows=500)
    original = data_file.read_bytes()
    # Keep the final 200 bytes (footer + PAR1 magic) and zero everything before it.
    data_file.write_bytes(b"\x00" * (len(original) - 200) + original[-200:])

    corrupt = verify_delta_table(table)

    assert corrupt == [data_file.name]


def test_verify_delta_table_ignores_a_non_delta_directory(tmp_path):
    """Test that a directory without a _delta_log is skipped rather than scanned."""
    plain = tmp_path / "plain"
    plain.mkdir()
    pq.write_table(pa.table({"a": [1]}), str(plain / "data.parquet"))

    assert verify_delta_table(plain) == []


def test_verify_delta_table_skips_checkpoint_files(tmp_path):
    """Test that transaction-log checkpoints are not reported as data corruption.

    Checkpoint damage is handled separately by heal_corrupt_delta_checkpoints, so counting it here
    would send the caller down the wrong recovery path.
    """
    table, _ = _write_delta_like_table(tmp_path)
    (table / "_delta_log" / "00000000000000000001.checkpoint.parquet").write_bytes(b"not a parquet file")

    assert verify_delta_table(table) == []


def test_is_corruption_error_matches_real_signatures():
    """Test that the real failure signatures from the incident are recognised."""
    assert is_corruption_error("org.apache.spark.SparkException: [FAILED_READ_FILE.NO_HINT] ...")
    assert is_corruption_error("Required field 'uncompressed_page_size' was not found")
    assert is_corruption_error("part-00000-0913d207-ad5c-4111-b1eb-30e3ed00412b-c000.snappy.parquet")
    assert is_corruption_error("org.apache.hadoop.fs.ChecksumException: Checksum error")


def test_is_corruption_error_ignores_unrelated_failures():
    """Test that ordinary errors do not trigger the recovery path."""
    assert not is_corruption_error("ValueError: Invalid date format. Please use YYYY-MM-DD format.")
    assert not is_corruption_error("Soda quality scan failed for silver_metrics with code 2.")
    assert not is_corruption_error("ConcurrentAppendException: Files were added by a concurrent update")


def test_find_archive_dates_parses_and_sorts_execution_dates(tmp_path):
    """Test that archive execution dates are discovered oldest first, ignoring stray files."""
    archive = tmp_path / "archive"
    archive.mkdir()
    for name in [
        "tickers_2026-09-20.parquet",
        "tickers_2026-06-21.parquet",
        "tickers_2026-08-01.parquet",
        "tickers_not-a-date.parquet",
        "ticker_metadata_2026-09-20.parquet",
    ]:
        (archive / name).touch()

    assert find_archive_dates(archive, "Prices") == ["2026-06-21", "2026-08-01", "2026-09-20"]
    assert find_archive_dates(archive, "Metadata") == ["2026-09-20"]


def test_replay_archive_to_bronze_returns_false_when_file_is_absent(tmp_path):
    """Test that replaying a date with no archived file is reported rather than raising."""
    archive = tmp_path / "archive"
    archive.mkdir()
    bronze = tmp_path / "bronze"
    bronze.mkdir()

    assert replay_archive_to_bronze("2026-09-20", {"archive": archive, "bronze": bronze}, "Prices") is False


def _archive_with_dates(tmp_path, dates, domain="Prices"):
    """Create an archive directory holding one (empty) archived file per execution date."""
    archive = tmp_path / "archive"
    archive.mkdir(exist_ok=True)
    prefix = "tickers_" if domain == "Prices" else "ticker_metadata_"
    for exec_date in dates:
        (archive / f"{prefix}{exec_date}.parquet").touch()
    return archive


@patch("src.streaming.utils.replay_archive_to_bronze", return_value=True)
@patch("src.streaming.utils.read_delta_table")
def test_recover_bronze_replays_only_dates_at_or_after_the_watermark(mock_read, mock_replay, tmp_path):
    """Test that recovery replays from the watermark instead of the whole archive.

    Inclusive of the watermark itself: the commit holding it may have been partially discarded by
    the rollback, and a duplicate replay is deduplicated by Silver while a gap would be permanent.
    """
    archive = _archive_with_dates(tmp_path, ["2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23"])
    mock_read.return_value.agg.return_value.collect.return_value = [["2026-09-21"]]

    replayed = recover_bronze_from_archive(
        paths={"archive": archive, "bronze": tmp_path / "bronze"},
        domain_name="Prices",
        watermark_column="date",
        spark=MagicMock(),
    )

    assert replayed == 3
    replayed_dates = [call.args[0] for call in mock_replay.call_args_list]
    assert replayed_dates == ["2026-09-21", "2026-09-22", "2026-09-23"]


@patch("src.streaming.utils.replay_archive_to_bronze", return_value=True)
@patch("src.streaming.utils.read_delta_table", side_effect=Exception("Path does not exist"))
def test_recover_bronze_replays_everything_when_the_watermark_is_unreadable(mock_read, mock_replay, tmp_path):
    """Test that an unreadable table after rollback falls back to a full archive replay."""
    archive = _archive_with_dates(tmp_path, ["2026-09-18", "2026-09-21"])

    replayed = recover_bronze_from_archive(
        paths={"archive": archive, "bronze": tmp_path / "bronze"},
        domain_name="Prices",
        watermark_column="date",
        spark=MagicMock(),
    )

    assert replayed == 2
    assert [call.args[0] for call in mock_replay.call_args_list] == ["2026-09-18", "2026-09-21"]


@patch("src.streaming.utils.replay_archive_to_bronze", side_effect=[Exception("write failed"), True])
@patch("src.streaming.utils.read_delta_table")
def test_recover_bronze_continues_after_a_failed_replay(mock_read, mock_replay, tmp_path):
    """Test that one unreadable archived file does not abort the remaining replays.

    The caller still fails loudly on the original corruption, so a partial recovery is reported
    rather than masking it with the replay error.
    """
    archive = _archive_with_dates(tmp_path, ["2026-09-21", "2026-09-22"])
    mock_read.return_value.agg.return_value.collect.return_value = [["2026-09-21"]]

    replayed = recover_bronze_from_archive(
        paths={"archive": archive, "bronze": tmp_path / "bronze"},
        domain_name="Prices",
        watermark_column="date",
        spark=MagicMock(),
    )

    assert replayed == 1
    assert mock_replay.call_count == 2


@patch("src.streaming.utils.replay_archive_to_bronze")
def test_recover_bronze_does_nothing_without_an_archive(mock_replay, tmp_path):
    """Test that an empty archive is reported rather than raising or replaying blindly."""
    archive = _archive_with_dates(tmp_path, [])

    assert (
        recover_bronze_from_archive(
            paths={"archive": archive, "bronze": tmp_path / "bronze"},
            domain_name="Prices",
            watermark_column="date",
            spark=MagicMock(),
        )
        == 0
    )
    mock_replay.assert_not_called()
