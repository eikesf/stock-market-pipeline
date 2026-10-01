"""Declarative row-level data-quality rules and quarantine mechanics for the Silver layer.

A row that breaches any rule is excluded from its Silver table entirely and written to the
matching ``*_rejected`` Delta table with every reason it breached, rather than having the
offending column nulled out in place. Only the essential columns (ticker plus the domain's
date column) are exempt: a null there fails the whole run instead of quarantining the row,
because a row without an identity cannot be meaningfully audited or reprocessed.

Every bound here mirrors a check in the matching ``soda/contracts/silver_*.yml`` contract.
The Soda contracts are intentionally left untouched and keep running after the write as an
independent backstop: once a bound is mirrored here, breaching rows never reach the valid
table, so the corresponding Soda check should stop firing in practice.
"""

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    array,
    array_compact,
    coalesce,
    col,
    concat,
    current_timestamp,
    lit,
    size,
    when,
)
from pyspark.sql.functions import sum as spark_sum

# Shared across domains: tickers are uppercased/trimmed before validation.
TICKER_FORMAT_REGEX = r"^[A-Z0-9\.\-]+$"

# yfinance leaves unresolved string fields as this literal rather than null.
PLACEHOLDER_VALUE = "N/A"

REJECTION_REASONS_COLUMN = "rejection_reasons"

# Promotes the prices OHLC consistency checks from Soda WARN to a quarantine trigger. Soda only
# warns because failing a whole batch over a few rows was too blunt, which row-level quarantine
# now solves properly. Flip to False to let structurally inconsistent OHLC rows through again.
QUARANTINE_OHLC_INCONSISTENCY = True


class RequiredColumnMissingError(ValueError):
    """Raised when an essential column is null for at least one row in the incoming batch.

    Subclasses ``ValueError`` so it flows through the existing ``except Exception`` handling in
    each ``run_*`` pipeline function without special-casing.
    """


@dataclass(frozen=True, eq=False)
class QualityRule:
    """A single row-level quality rule.

    Both columns are unbound Spark expressions referencing columns by name, so rules are built
    once at import time and reused against any DataFrame exposing those columns.

    Attributes:
        name: Short identifier used in logs and tests, e.g. ``"roa_range"``.
        breach_condition: Boolean expression that is True for rows violating this rule.
        reason: String expression yielding the human-readable reason where the rule is breached
            and null elsewhere, so breached reasons can be collected per row.
    """

    name: str
    breach_condition: Column
    reason: Column


def _reason_with_value(message: str, column: str) -> Column:
    """Append the offending value to a reason message.

    ``concat`` returns null if any operand is null, which would silently erase the reason for a
    row breaching on a null value, so the value is coalesced to a literal first.

    Args:
        message: Human-readable description of the rule that was breached.
        column: Column whose offending value should be appended.

    Returns:
        A string column combining the message and the offending value.
    """
    return concat(lit(f"{message}, value="), coalesce(col(column).cast("string"), lit("NULL")))


def range_rule(
    column: str,
    min_value: float | None = None,
    max_value: float | None = None,
    min_inclusive: bool = True,
    max_inclusive: bool = True,
) -> QualityRule:
    """Build a numeric bound rule. Nulls never breach.

    Inclusivity describes whether the bound itself is an allowed value, matching how the Soda
    contracts are written: ``min(x) >= 0`` is ``min_value=0, min_inclusive=True`` (0 is allowed),
    while ``min(x) > -1000`` is ``min_value=-1000, min_inclusive=False`` (-1000 is not).

    Args:
        column: Column to bound.
        min_value: Lower bound, or None for no lower bound.
        max_value: Upper bound, or None for no upper bound.
        min_inclusive: Whether ``min_value`` itself is an allowed value.
        max_inclusive: Whether ``max_value`` itself is an allowed value.

    Returns:
        The configured rule.

    Raises:
        ValueError: If neither bound is supplied.
    """
    target = col(column)
    breaches: list[Column] = []
    descriptions: list[str] = []

    if min_value is not None:
        breaches.append(target < min_value if min_inclusive else target <= min_value)
        descriptions.append(f">= {min_value}" if min_inclusive else f"> {min_value}")
    if max_value is not None:
        breaches.append(target > max_value if max_inclusive else target >= max_value)
        descriptions.append(f"<= {max_value}" if max_inclusive else f"< {max_value}")
    if not breaches:
        raise ValueError(f"range_rule for {column!r} requires at least one of min_value/max_value")

    breach_condition = target.isNotNull()
    any_breach = breaches[0]
    for extra in breaches[1:]:
        any_breach = any_breach | extra
    breach_condition = breach_condition & any_breach

    message = f"{column} outside expected range (needs {' and '.join(descriptions)})"
    return QualityRule(
        name=f"{column}_range",
        breach_condition=breach_condition,
        reason=when(breach_condition, _reason_with_value(message, column)),
    )


def format_rule(column: str, regex: str) -> QualityRule:
    """Build a regex format rule. Nulls never breach.

    Args:
        column: Column to validate.
        regex: Pattern the value must match in full.

    Returns:
        The configured rule.
    """
    target = col(column)
    breach_condition = target.isNotNull() & ~target.rlike(regex)
    message = f"{column} does not match required format {regex}"
    return QualityRule(
        name=f"{column}_format",
        breach_condition=breach_condition,
        reason=when(breach_condition, _reason_with_value(message, column)),
    )


def populated_rule(column: str, placeholder: str = PLACEHOLDER_VALUE) -> QualityRule:
    """Build a rule requiring a real value: null and the source placeholder both breach.

    Used for the metadata columns that were previously hard-dropped (short_name, sector,
    exchange, currency) and are now quarantined with an audit trail instead.

    Args:
        column: Column that must carry a real value.
        placeholder: Sentinel the source system uses for "unknown".

    Returns:
        The configured rule.
    """
    target = col(column)
    breach_condition = target.isNull() | (target == placeholder)
    return QualityRule(
        name=f"{column}_populated",
        breach_condition=breach_condition,
        reason=when(breach_condition, lit(f"{column} is missing or set to the '{placeholder}' placeholder")),
    )


def not_null_rule(column: str) -> QualityRule:
    """Build a rule requiring a non-null value, with no opinion on placeholders.

    Applied to columns the Gold layer declares non-Nullable: letting a null through would make
    ClickHouse coerce it to a zero/empty value on insert, silently corrupting Gold. Unlike
    :func:`populated_rule` this accepts a placeholder such as ``"N/A"``, which some columns
    (notably ``isin``) treat as a legitimate value.

    Args:
        column: Column that must be non-null.

    Returns:
        The configured rule.
    """
    breach_condition = col(column).isNull()
    return QualityRule(
        name=f"{column}_not_null",
        breach_condition=breach_condition,
        reason=when(breach_condition, lit(f"{column} is missing and is required by the Gold schema")),
    )


def custom_rule(name: str, breach_condition: Column, message: str) -> QualityRule:
    """Build a rule from an arbitrary expression, for cross-column invariants.

    Args:
        name: Short identifier for the rule.
        breach_condition: Boolean expression that is True for violating rows.
        message: Static reason recorded for violating rows.

    Returns:
        The configured rule.
    """
    return QualityRule(
        name=name,
        breach_condition=breach_condition,
        reason=when(breach_condition, lit(message)),
    )


def require_columns(df: DataFrame, columns: list[str], domain: str) -> None:
    """Fail the whole run if any row is missing an essential column.

    Deliberately not a silent ``na.drop``: a row without a ticker or date cannot be audited or
    reprocessed, so it signals a broken extraction rather than one bad record to quarantine.

    Args:
        df: DataFrame to check.
        columns: Essential column names that must be non-null on every row.
        domain: Domain name used in the error message, e.g. ``"metrics"``.

    Raises:
        RequiredColumnMissingError: If any listed column is null on at least one row.
    """
    null_counts = df.select(
        *[spark_sum(when(col(column).isNull(), 1).otherwise(0)).alias(column) for column in columns]
    ).collect()[0]

    # sum() over an empty DataFrame yields null rather than 0.
    offending = {column: null_counts[column] for column in columns if (null_counts[column] or 0) > 0}
    if offending:
        raise RequiredColumnMissingError(
            f"Silver {domain}: {offending} row(s) are missing an essential column. "
            "ticker and the domain date column are required and are never quarantined."
        )


def classify(df: DataFrame, rules: list[QualityRule]) -> DataFrame:
    """Attach a ``rejection_reasons`` array holding every rule the row breached.

    Args:
        df: DataFrame to classify.
        rules: Rules to evaluate against each row.

    Returns:
        The DataFrame with an added ``rejection_reasons`` array column, empty for clean rows.
    """
    reasons = [rule.reason for rule in rules]
    return df.withColumn(REJECTION_REASONS_COLUMN, array_compact(array(*reasons)))


def split_valid_rejected(classified_df: DataFrame, pipeline_exec_date: str) -> tuple[DataFrame, DataFrame]:
    """Split a classified DataFrame into its valid and quarantined halves.

    Args:
        classified_df: DataFrame returned by :func:`classify`.
        pipeline_exec_date: Execution date of the run, recorded on every quarantined row.

    Returns:
        A ``(valid_df, rejected_df)`` tuple. ``valid_df`` carries the original columns unchanged.
        ``rejected_df`` keeps every original column and adds ``pipeline_exec_date``,
        ``rejected_at`` and ``rejection_reasons``.
    """
    reasons = col(REJECTION_REASONS_COLUMN)
    valid_df = classified_df.filter(size(reasons) == 0).drop(REJECTION_REASONS_COLUMN)
    rejected_df = (
        classified_df.filter(size(reasons) > 0)
        .withColumn("pipeline_exec_date", lit(pipeline_exec_date).cast("date"))
        .withColumn("rejected_at", current_timestamp())
    )
    return valid_df, rejected_df


# =============================================================================================
# Domain rule sets. Each entry cites the soda/contracts/silver_*.yml check it mirrors.
#
# These are functions rather than module-level constants because pyspark's col() requires an
# active SparkContext, which does not exist at import time.
# =============================================================================================

PRICES_REQUIRED_COLUMNS = ["ticker", "date"]
METRICS_REQUIRED_COLUMNS = ["ticker", "extraction_date"]
METADATA_REQUIRED_COLUMNS = ["ticker", "extraction_date"]

# Columns declared non-Nullable in the Gold ClickHouse tables (src/db_init/init.sql). A null here
# is quarantined rather than passed on, because ClickHouse would coerce it to a zero/empty value.
# fact_company_metrics makes every ratio Nullable, so metrics only needs its ingestion timestamp.
PRICES_GOLD_REQUIRED_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "dividends",
    "stock_splits",
    "ingestion_timestamp",
]
METRICS_GOLD_REQUIRED_COLUMNS = ["ingestion_timestamp"]
METADATA_GOLD_REQUIRED_COLUMNS = ["industry", "country", "isin", "ingestion_timestamp"]


def prices_rules() -> list[QualityRule]:
    """Build the quality rules for the Silver prices table.

    Returns:
        Rules mirroring the bounds in soda/contracts/silver_prices_contract.yml.
    """
    # Every one of these is non-Nullable in stock_market.fact_prices, so a null cannot be allowed
    # through to Gold.
    rules = [not_null_rule(column) for column in PRICES_GOLD_REQUIRED_COLUMNS]
    rules += [
        range_rule("open", min_value=0),  # min(open) >= 0
        range_rule("high", min_value=0),  # min(high) >= 0
        range_rule("low", min_value=0),  # min(low) >= 0
        range_rule("close", min_value=0),  # min(close) >= 0
        range_rule("adj_close", min_value=0),  # min(adj_close) >= 0
        range_rule("volume", min_value=0),  # min(volume) >= 0
        range_rule("dividends", min_value=0.0),  # min(dividends) >= 0.0
        range_rule("stock_splits", min_value=0.0),  # min(stock_splits) >= 0.0
        format_rule("ticker", TICKER_FORMAT_REGEX),  # "Check for invalid ticker format"
    ]

    if QUARANTINE_OHLC_INCONSISTENCY:
        # Soda raises these as warnings only; quarantining removes the offending rows instead.
        traded = (col("volume") > 0) & (col("open") > 0)
        rules += [
            custom_rule(
                "high_not_highest",
                traded & ((col("high") < col("low")) | (col("high") < col("open")) | (col("high") < col("close"))),
                "high is not the highest value of the trading day",
            ),
            custom_rule(
                "low_not_lowest",
                traded & ((col("low") > col("open")) | (col("low") > col("close"))),
                "low is not the lowest value of the trading day",
            ),
        ]
    return rules


def metrics_rules() -> list[QualityRule]:
    """Build the quality rules for the Silver metrics table.

    A null metric never breaches: the Soda contract tolerates 25-35% missing values on several of
    these columns, so only an implausible value that is actually present is quarantined.

    Returns:
        Rules mirroring the bounds in soda/contracts/silver_metrics_contract.yml.
    """
    rules = [not_null_rule(column) for column in METRICS_GOLD_REQUIRED_COLUMNS]
    rules += [
        range_rule("market_cap", min_value=0),  # min(market_cap) >= 0
        range_rule("shares_outstanding", min_value=0),  # min(shares_outstanding) >= 0
        range_rule("total_debt", min_value=0),  # min(total_debt) >= 0
        range_rule("total_cash", min_value=0),  # min(total_cash) >= 0
        range_rule("dividend_yield", min_value=0),  # min(dividend_yield) >= 0
        range_rule("price_to_sales", min_value=0),  # min(price_to_sales) >= 0
        range_rule("current_ratio", min_value=0),  # min(current_ratio) >= 0
        range_rule("asset_turnover", min_value=0),  # min(asset_turnover) >= 0
        range_rule("trailing_pe", min_value=0),  # min(trailing_pe) >= 0
        range_rule("ebitda", min_value=-1000000000000, min_inclusive=False),  # min(ebitda) > -1e12
        range_rule("net_income_to_common", min_value=-1000000000000, min_inclusive=False),  # min(...) > -1e12
        range_rule("book_value", min_value=-10000, min_inclusive=False),  # min(book_value) > -10000
        range_rule("trailing_eps", min_value=-1000, min_inclusive=False),  # min(trailing_eps) > -1000
        range_rule("peg_ratio", min_value=-100.0, min_inclusive=False),  # min(peg_ratio) > -100.0
        range_rule("price_to_book", min_value=-1000.0, min_inclusive=False),  # min(price_to_book) > -1000.0
        range_rule(
            "debt_to_equity", min_value=-1000.0, max_value=500000.0, min_inclusive=False, max_inclusive=False
        ),  # min(debt_to_equity) > -1000.0, max(debt_to_equity) < 500000.0
        range_rule(
            "operating_margins", min_value=-200.0, max_value=2.5, min_inclusive=False
        ),  # min(operating_margins) > -200.0, max(operating_margins) <= 2.5
        range_rule(
            "ebitda_margins", min_value=-200.0, max_value=2.5, min_inclusive=False
        ),  # min(ebitda_margins) > -200.0, max(ebitda_margins) <= 2.5
        range_rule(
            "profit_margins", min_value=-200.0, max_value=5.5, min_inclusive=False
        ),  # min(profit_margins) > -200.0, max(profit_margins) <= 5.5
        range_rule(
            "gross_margins", min_value=-200.0, max_value=2.5, min_inclusive=False
        ),  # min(gross_margins) > -200.0, max(gross_margins) <= 2.5
        range_rule("roa", min_value=-50.0, max_value=50.0, min_inclusive=False),  # min(roa) > -50, max(roa) <= 50
        range_rule("roe", min_value=-100.0, max_value=100.0, min_inclusive=False),  # min(roe) > -100, max(roe) <= 100
        range_rule(
            "enterprise_to_ebitda", min_value=-5000.0, max_value=5000.0, min_inclusive=False, max_inclusive=False
        ),  # min(enterprise_to_ebitda) > -5000.0, max(enterprise_to_ebitda) < 5000.0
        range_rule(
            "enterprise_to_ebit", min_value=-5000.0, max_value=5000.0, min_inclusive=False, max_inclusive=False
        ),  # min(enterprise_to_ebit) > -5000.0, max(enterprise_to_ebit) < 5000.0
        format_rule("ticker", TICKER_FORMAT_REGEX),  # "Check for invalid ticker format"
    ]
    return rules


def metadata_rules() -> list[QualityRule]:
    """Build the quality rules for the Silver metadata table.

    The SCD Type 2 bookkeeping columns (start_date, end_date, is_active) are set programmatically
    after the split, so they are correct by construction and carry no rules. exchange and currency
    are validated after the standardization remap, so only genuinely unresolvable values breach.
    Deliberately no allow-list on exchange/currency: an unmapped but legitimate venue (e.g. LSE)
    still flows through, matching the standardization step's documented "keep others as is" intent.

    Returns:
        Rules mirroring the bounds in soda/contracts/silver_metadata_contract.yml.
    """
    # industry/country/isin are non-Nullable in dim_companies but, unlike the four columns below,
    # the standardization step has always let their "N/A" placeholder through, so only nulls breach.
    rules = [not_null_rule(column) for column in METADATA_GOLD_REQUIRED_COLUMNS]
    rules += [
        populated_rule("short_name"),  # missing_count(short_name) = 0
        populated_rule("sector"),  # missing_count(sector) = 0
        populated_rule("exchange"),  # missing_count(exchange) = 0
        populated_rule("currency"),  # missing_count(currency) = 0
        range_rule("full_time_employees", min_value=0),  # min(full_time_employees) >= 0
        format_rule("isin", r"^(N/A|[A-Z]{2}[A-Z0-9]{9}[0-9])$"),  # "Check for invalid ISIN format"
        format_rule("ticker", TICKER_FORMAT_REGEX),  # "Check for invalid ticker format"
    ]
    return rules
