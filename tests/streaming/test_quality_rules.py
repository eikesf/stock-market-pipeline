import pytest

from src.streaming.quality_rules import (
    RequiredColumnMissingError,
    classify,
    custom_rule,
    format_rule,
    not_null_rule,
    populated_rule,
    range_rule,
    require_columns,
    split_valid_rejected,
)


def _frame(spark_session, rows):
    """Build a small Spark DataFrame from a list of dicts.

    Deliberately not routed through pandas, which would coerce None to NaN or the string "nan".
    """
    return spark_session.createDataFrame(rows)


def test_range_rule_ignores_nulls(spark_session):
    """Test that a null value never breaches a range rule."""
    df = _frame(spark_session, [{"ticker": "A", "value": None}, {"ticker": "B", "value": -5.0}])

    classified = classify(df, [range_rule("value", min_value=0)])
    reasons = {row.ticker: row.rejection_reasons for row in classified.collect()}

    assert reasons["A"] == []
    assert any("value outside expected range" in reason for reason in reasons["B"])


def test_range_rule_bound_inclusivity(spark_session):
    """Test that inclusivity controls whether the bound value itself is allowed."""
    df = _frame(spark_session, [{"ticker": "AT_BOUND", "value": 0.0}])

    inclusive = classify(df, [range_rule("value", min_value=0, min_inclusive=True)]).collect()[0]
    exclusive = classify(df, [range_rule("value", min_value=0, min_inclusive=False)]).collect()[0]

    assert inclusive.rejection_reasons == []
    assert len(exclusive.rejection_reasons) == 1


def test_range_rule_reports_both_bounds(spark_session):
    """Test that a value above the ceiling breaches a two-sided range rule."""
    df = _frame(spark_session, [{"ticker": "HIGH", "value": 9.0}])

    classified = classify(df, [range_rule("value", min_value=-1.0, max_value=2.5)])

    assert any("value outside expected range" in reason for reason in classified.collect()[0].rejection_reasons)


def test_range_rule_requires_a_bound():
    """Test that building a range rule without any bound is rejected."""
    with pytest.raises(ValueError, match="at least one of min_value/max_value"):
        range_rule("value")


def test_format_rule_ignores_nulls_and_flags_mismatches(spark_session):
    """Test that a format rule skips nulls and flags non-matching values."""
    df = _frame(spark_session, [{"ticker": "A", "isin": None}, {"ticker": "B", "isin": "nope"}])

    classified = classify(df, [format_rule("isin", r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")])
    reasons = {row.ticker: row.rejection_reasons for row in classified.collect()}

    assert reasons["A"] == []
    assert any("isin does not match required format" in reason for reason in reasons["B"])


def test_populated_rule_flags_null_and_placeholder(spark_session):
    """Test that both null and the placeholder sentinel breach a populated rule."""
    df = _frame(
        spark_session,
        [{"ticker": "A", "sector": None}, {"ticker": "B", "sector": "N/A"}, {"ticker": "C", "sector": "Tech"}],
    )

    classified = classify(df, [populated_rule("sector")])
    reasons = {row.ticker: row.rejection_reasons for row in classified.collect()}

    assert len(reasons["A"]) == 1
    assert len(reasons["B"]) == 1
    assert reasons["C"] == []


def test_not_null_rule_allows_placeholder(spark_session):
    """Test that a not-null rule accepts the placeholder but rejects null."""
    df = _frame(spark_session, [{"ticker": "A", "isin": None}, {"ticker": "B", "isin": "N/A"}])

    classified = classify(df, [not_null_rule("isin")])
    reasons = {row.ticker: row.rejection_reasons for row in classified.collect()}

    assert any("isin is missing" in reason for reason in reasons["A"])
    assert reasons["B"] == []


def test_classify_accumulates_every_breached_reason(spark_session):
    """Test that a row breaching several rules collects one reason per rule."""
    df = _frame(spark_session, [{"ticker": "BAD", "first": -1.0, "second": -2.0}])

    classified = classify(df, [range_rule("first", min_value=0), range_rule("second", min_value=0)])
    reasons = classified.collect()[0].rejection_reasons

    assert len(reasons) == 2
    assert any("first" in reason for reason in reasons)
    assert any("second" in reason for reason in reasons)


def test_custom_rule_supports_cross_column_conditions(spark_session):
    """Test that a custom rule can compare two columns."""
    from pyspark.sql.functions import col

    df = _frame(spark_session, [{"ticker": "BAD", "high": 1.0, "low": 5.0}, {"ticker": "OK", "high": 5.0, "low": 1.0}])

    rule = custom_rule("high_below_low", col("high") < col("low"), "high is below low")
    reasons = {row.ticker: row.rejection_reasons for row in classify(df, [rule]).collect()}

    assert reasons["BAD"] == ["high is below low"]
    assert reasons["OK"] == []


def test_split_valid_rejected_partitions_rows_and_adds_audit_columns(spark_session):
    """Test that the split separates clean rows and stamps quarantined rows with audit columns."""
    df = _frame(spark_session, [{"ticker": "OK", "value": 1.0}, {"ticker": "BAD", "value": -1.0}])

    classified = classify(df, [range_rule("value", min_value=0)])
    valid_df, rejected_df = split_valid_rejected(classified, pipeline_exec_date="2026-05-28")

    assert [row.ticker for row in valid_df.collect()] == ["OK"]
    assert "rejection_reasons" not in valid_df.columns

    rejected = rejected_df.collect()
    assert len(rejected) == 1
    assert rejected[0].ticker == "BAD"
    # The original value survives for investigation, alongside the audit columns
    assert rejected[0].value == -1.0
    assert rejected[0].pipeline_exec_date.isoformat() == "2026-05-28"
    assert rejected[0].rejected_at is not None


def test_split_valid_rejected_handles_a_fully_clean_batch(spark_session):
    """Test that a batch with no breaches yields an empty quarantine frame."""
    df = _frame(spark_session, [{"ticker": "OK", "value": 1.0}])

    valid_df, rejected_df = split_valid_rejected(
        classify(df, [range_rule("value", min_value=0)]), pipeline_exec_date="2026-05-28"
    )

    assert valid_df.count() == 1
    assert rejected_df.count() == 0


def test_require_columns_passes_when_all_present(spark_session):
    """Test that a batch with no nulls in the essential columns passes."""
    df = _frame(spark_session, [{"ticker": "A", "date": "2026-05-28"}])

    require_columns(df, ["ticker", "date"], domain="prices")


def test_require_columns_raises_and_names_offending_columns(spark_session):
    """Test that a null in an essential column fails loudly and names the column."""
    df = _frame(spark_session, [{"ticker": "A", "date": "2026-05-28"}, {"ticker": None, "date": None}])

    with pytest.raises(RequiredColumnMissingError, match="ticker"):
        require_columns(df, ["ticker", "date"], domain="prices")


def test_require_columns_handles_an_empty_batch(spark_session):
    """Test that an empty batch does not trip the essential-column check.

    Regression test: sum() over an empty DataFrame returns null rather than 0, which would raise a
    TypeError when compared against a number.
    """
    df = _frame(spark_session, [{"ticker": "A", "date": "2026-05-28"}]).filter("ticker = 'MISSING'")

    require_columns(df, ["ticker", "date"], domain="prices")
