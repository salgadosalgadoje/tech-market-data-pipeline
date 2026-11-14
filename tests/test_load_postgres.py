"""Integration and Unit tests for PostgreSQL Warehouse Ingestion (Gold Layer).

Tests the Medallion Architecture Phase 3 Gold Layer:
- SQLAlchemy ORM data model (`MarketDailyMetrics`) and schema indexing.
- Dialect-aware idempotent UPSERT (`INSERT ... ON CONFLICT (ticker, date) DO UPDATE`).
- Strict verification of single-record conflict resolution (1 record preserved, values updated).
- Batch idempotency across multiple re-runs.
- S3 Silver layer Parquet reading with moto (AWS S3 mock).
- End-to-end integration with in-memory SQLite database.
"""

from datetime import date, datetime, timedelta, timezone
import io
from typing import Any, Dict, Generator
import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

try:
    from moto import mock_aws
except ImportError:
    from moto import mock_s3 as mock_aws

import boto3

from src.load_postgres import (
    Base,
    MarketDailyMetrics,
    PostgresDataLoader,
    PostgresLoaderConfig,
)

# Test Constants
TEST_BUCKET = "test-market-data-silver-bucket"
TEST_REGION = "us-east-1"
TEST_SILVER_PREFIX = "silver/market_data"


@pytest.fixture(autouse=True)
def aws_env_setup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set mock AWS environment variables for isolation."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing_access_key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing_secret_key")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing_security_token")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing_session_token")
    monkeypatch.setenv("AWS_DEFAULT_REGION", TEST_REGION)
    monkeypatch.setenv("S3_BUCKET_NAME", TEST_BUCKET)
    monkeypatch.setenv("S3_SILVER_PREFIX", TEST_SILVER_PREFIX)
    monkeypatch.setenv("TARGET_TICKERS", "NVDA,TSM")


@pytest.fixture
def sqlite_engine() -> Generator[Engine, None, None]:
    """Provide an in-memory SQLite SQLAlchemy engine for integration testing."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        future=True,
    )
    # Create tables
    Base.metadata.create_all(engine)
    yield engine
    # Teardown
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture
def db_session(sqlite_engine: Engine) -> Generator[Session, None, None]:
    """Provide a scoped database session for querying test results."""
    session_factory = sessionmaker(bind=sqlite_engine)
    session = session_factory()
    yield session
    session.close()


@pytest.fixture
def mock_s3_client() -> Generator[Any, None, None]:
    """Create a mocked S3 environment using moto."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=TEST_REGION)
        s3.create_bucket(Bucket=TEST_BUCKET)
        yield s3


@pytest.fixture
def sample_silver_dataframe() -> pd.DataFrame:
    """Create a realistic Silver DataFrame with technical indicators."""
    base_date = date(2026, 3, 1)
    dates = [base_date + timedelta(days=i) for i in range(5)]
    data = {
        "Date": [d.strftime("%Y-%m-%d") for d in dates],
        "ticker": ["NVDA"] * 5,
        "Open": [120.0 + i for i in range(5)],
        "High": [125.0 + i for i in range(5)],
        "Low": [118.0 + i for i in range(5)],
        "Close": [122.5 + i for i in range(5)],
        "Volume": [1000000 + (i * 50000) for i in range(5)],
        "SMA_7": [121.0 + i for i in range(5)],
        "SMA_30": [115.0 + i for i in range(5)],
        "daily_return": [0.0, 0.012, -0.005, 0.018, 0.009],
        "volatility_7": [0.0, 0.005, 0.008, 0.012, 0.007],
        "transformed_at_utc": [datetime.now(timezone.utc).isoformat()] * 5,
    }
    return pd.DataFrame(data)


@pytest.fixture
def data_loader(sqlite_engine: Engine, mock_s3_client: Any) -> PostgresDataLoader:
    """Instantiate PostgresDataLoader configured with in-memory SQLite and moto S3."""
    config = PostgresLoaderConfig(
        bucket_name=TEST_BUCKET,
        silver_prefix=TEST_SILVER_PREFIX,
        target_tickers=["NVDA", "TSM"],
    )
    return PostgresDataLoader(
        config=config,
        engine=sqlite_engine,
        s3_client=mock_s3_client,
    )


# =====================================================================
# Unit & Integration Tests: Schema, ORM & Table Structure
# =====================================================================

def test_table_creation_and_composite_primary_key(sqlite_engine: Engine) -> None:
    """Verify that market_daily_metrics table is created with composite primary key (ticker, date)."""
    inspector = inspect(sqlite_engine)
    table_names = inspector.get_table_names()
    assert "market_daily_metrics" in table_names

    pk_constraint = inspector.get_pk_constraint("market_daily_metrics")
    pk_columns = pk_constraint.get("constrained_columns", [])
    assert set(pk_columns) == {"ticker", "date"}, f"Expected PK on (ticker, date), got {pk_columns}"

    # Verify column existence including technical indicators
    columns = {col["name"] for col in inspector.get_columns("market_daily_metrics")}
    assert "volatility_7" in columns, "Expected 'volatility_7' column in market_daily_metrics table"


def test_table_indexes_configuration(sqlite_engine: Engine) -> None:
    """Verify that required indexes are defined on (ticker, date), date, and ticker."""
    inspector = inspect(sqlite_engine)
    indexes = inspector.get_indexes("market_daily_metrics")
    index_names = {idx["name"] for idx in indexes}

    # Verify our custom indexes exist
    expected_indexes = {
        "ix_market_daily_metrics_ticker_date",
        "ix_market_daily_metrics_date",
        "ix_market_daily_metrics_ticker",
    }
    for expected in expected_indexes:
        assert expected in index_names, f"Expected index '{expected}' not found in table indexes: {index_names}"


# =====================================================================
# Strict Upsert Integration Tests
# =====================================================================

def test_strict_upsert_insert_then_update_single_record(
    data_loader: PostgresDataLoader, db_session: Session
) -> None:
    """Strictly verify the UPSERT idempotency logic.

    1. Insert initial record for ('NVDA', '2026-03-01').
    2. Verify 1 record exists with initial values.
    3. Insert updated record for the SAME ('NVDA', '2026-03-01') with modified metrics.
    4. Verify that the table still contains EXACTLY ONE record and values are updated.
    """
    initial_date = date(2026, 3, 1)

    initial_record = [
        {
            "ticker": "NVDA",
            "date": initial_date,
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 102.5,
            "volume": 1000000,
            "sma_7": 101.0,
            "sma_30": 98.0,
            "daily_return": 0.015,
            "volatility_7": 0.012,
            "transformed_at_utc": datetime(2026, 3, 1, 18, 0, 0, tzinfo=timezone.utc),
            "loaded_at_utc": datetime(2026, 3, 1, 18, 5, 0, tzinfo=timezone.utc),
        }
    ]

    # Step 1: Perform initial insertion
    count_1 = data_loader.upsert_records(initial_record)
    assert count_1 == 1

    # Step 2: Query database and confirm exactly 1 record with initial values
    stmt = select(MarketDailyMetrics).where(
        MarketDailyMetrics.ticker == "NVDA",
        MarketDailyMetrics.date == initial_date,
    )
    results = db_session.execute(stmt).scalars().all()
    assert len(results) == 1
    assert results[0].close == 102.5
    assert results[0].volume == 1000000
    assert results[0].sma_7 == 101.0

    # Step 3: Perform UPSERT update for the SAME ticker and date with updated financial metrics
    updated_record = [
        {
            "ticker": "NVDA",
            "date": initial_date,  # Conflict trigger
            "open": 101.0,
            "high": 112.0,
            "low": 100.5,
            "close": 111.8,  # Changed
            "volume": 2500000,  # Changed
            "sma_7": 106.2,  # Changed
            "sma_30": 100.1,  # Changed
            "daily_return": 0.0907,  # Changed
            "volatility_7": 0.035,  # Changed
            "transformed_at_utc": datetime(2026, 3, 1, 20, 0, 0, tzinfo=timezone.utc),
            "loaded_at_utc": datetime(2026, 3, 1, 20, 10, 0, tzinfo=timezone.utc),
        }
    ]

    count_2 = data_loader.upsert_records(updated_record)
    assert count_2 == 1

    # Expire session cache to ensure fresh read from database engine
    db_session.expire_all()

    # Step 4: Strict assertion - table must maintain a SINGLE record with UPDATED values
    updated_results = db_session.execute(stmt).scalars().all()
    assert len(updated_results) == 1, (
        f"Upsert failed idempotency! Expected exactly 1 record, found {len(updated_results)}"
    )

    row = updated_results[0]
    assert row.ticker == "NVDA"
    assert row.date == initial_date
    assert row.close == 111.8, f"Expected close=111.8, got {row.close}"
    assert row.volume == 2500000, f"Expected volume=2500000, got {row.volume}"
    assert row.sma_7 == 106.2, f"Expected sma_7=106.2, got {row.sma_7}"
    assert row.sma_30 == 100.1, f"Expected sma_30=100.1, got {row.sma_30}"
    assert row.daily_return == 0.0907, f"Expected daily_return=0.0907, got {row.daily_return}"
    assert row.volatility_7 == 0.035, f"Expected volatility_7=0.035, got {row.volatility_7}"


def test_batch_upsert_idempotence(
    data_loader: PostgresDataLoader,
    sample_silver_dataframe: pd.DataFrame,
    db_session: Session,
) -> None:
    """Verify that running batch upsert twice with the same data causes zero duplicate rows."""
    records = data_loader.prepare_records(sample_silver_dataframe)
    total_records = len(records)
    assert total_records == 5

    # First load
    data_loader.upsert_records(records)
    all_rows = db_session.execute(select(MarketDailyMetrics)).scalars().all()
    assert len(all_rows) == 5

    # Second load (identical data)
    data_loader.upsert_records(records)
    all_rows_after_rerun = db_session.execute(select(MarketDailyMetrics)).scalars().all()
    assert len(all_rows_after_rerun) == 5, "Duplicate records were introduced during re-run!"


def test_batch_upsert_partial_overlap_and_new_records(
    data_loader: PostgresDataLoader,
    db_session: Session,
) -> None:
    """Verify that upsert properly updates existing records while inserting newly introduced dates."""
    initial_records = [
        {
            "ticker": "TSM",
            "date": date(2026, 3, 1),
            "open": 140.0,
            "high": 145.0,
            "low": 139.0,
            "close": 142.0,
            "volume": 500000,
            "sma_7": 140.0,
            "sma_30": None,
            "daily_return": 0.0,
            "transformed_at_utc": datetime.now(timezone.utc),
            "loaded_at_utc": datetime.now(timezone.utc),
        },
        {
            "ticker": "TSM",
            "date": date(2026, 3, 2),
            "open": 142.0,
            "high": 146.0,
            "low": 141.0,
            "close": 144.0,
            "volume": 600000,
            "sma_7": 142.0,
            "sma_30": None,
            "daily_return": 0.014,
            "transformed_at_utc": datetime.now(timezone.utc),
            "loaded_at_utc": datetime.now(timezone.utc),
        },
    ]

    data_loader.upsert_records(initial_records)
    assert len(db_session.execute(select(MarketDailyMetrics)).scalars().all()) == 2

    # Second batch: updates 2026-03-02 and inserts 2026-03-03
    second_batch = [
        {
            "ticker": "TSM",
            "date": date(2026, 3, 2),
            "open": 142.0,
            "high": 150.0,
            "low": 141.0,
            "close": 149.5,  # Modified close
            "volume": 750000,
            "sma_7": 145.0,
            "sma_30": None,
            "daily_return": 0.052,
            "transformed_at_utc": datetime.now(timezone.utc),
            "loaded_at_utc": datetime.now(timezone.utc),
        },
        {
            "ticker": "TSM",
            "date": date(2026, 3, 3),  # Brand new date
            "open": 150.0,
            "high": 152.0,
            "low": 148.0,
            "close": 151.0,
            "volume": 800000,
            "sma_7": 147.0,
            "sma_30": None,
            "daily_return": 0.01,
            "transformed_at_utc": datetime.now(timezone.utc),
            "loaded_at_utc": datetime.now(timezone.utc),
        },
    ]

    data_loader.upsert_records(second_batch)

    rows = db_session.execute(select(MarketDailyMetrics).order_by(MarketDailyMetrics.date)).scalars().all()
    assert len(rows) == 3

    # Check that date 2026-03-02 was updated
    date_2 = next(r for r in rows if r.date == date(2026, 3, 2))
    assert date_2.close == 149.5
    assert date_2.volume == 750000

    # Check that date 2026-03-03 was inserted
    date_3 = next(r for r in rows if r.date == date(2026, 3, 3))
    assert date_3.close == 151.0


# =====================================================================
# Unit Tests: Record Preparation & Normalization
# =====================================================================

def test_prepare_records_converts_types_and_handles_nan(
    data_loader: PostgresDataLoader,
) -> None:
    """Verify column normalization, date parsing, and conversion of NaN to None."""
    raw_df = pd.DataFrame(
        {
            "Date": ["2026-03-01", "2026-03-02"],
            "ticker": ["nvda", "NVDA "],  # Needs lowercase/whitespace cleanup
            "Open": [100.5, 102.0],
            "High": [105.0, 106.0],
            "Low": [99.0, 101.0],
            "Close": [104.0, 105.5],
            "Volume": [1000, 2000],
            "SMA_7": [np.nan, 103.5],  # Contains NaN
            "SMA_30": [np.nan, np.nan],  # All NaN
            "daily_return": [0.0, np.nan],
            "transformed_at_utc": ["2026-03-02T12:00:00Z", None],
        }
    )

    records = data_loader.prepare_records(raw_df)
    assert len(records) == 2

    # Check record 0
    r0 = records[0]
    assert r0["ticker"] == "NVDA"
    assert r0["date"] == date(2026, 3, 1)
    assert isinstance(r0["date"], date)
    assert r0["sma_7"] is None  # NaN replaced by None
    assert r0["sma_30"] is None
    assert r0["daily_return"] == 0.0
    assert r0["loaded_at_utc"] is not None

    # Check record 1
    r1 = records[1]
    assert r1["ticker"] == "NVDA"
    assert r1["date"] == date(2026, 3, 2)
    assert r1["sma_7"] == 103.5
    assert r1["sma_30"] is None


def test_prepare_records_empty_dataframe_raises(data_loader: PostgresDataLoader) -> None:
    """Verify ValueError is raised if input DataFrame is empty or None."""
    with pytest.raises(ValueError, match="Input DataFrame is empty or None"):
        data_loader.prepare_records(pd.DataFrame())

    with pytest.raises(ValueError, match="Input DataFrame is empty or None"):
        data_loader.prepare_records(None)  # type: ignore[arg-type]


def test_prepare_records_missing_mandatory_columns_raises(
    data_loader: PostgresDataLoader,
) -> None:
    """Verify ValueError is raised if mandatory columns are absent."""
    incomplete_df = pd.DataFrame(
        {
            "Date": ["2026-03-01"],
            "ticker": ["NVDA"],
            # Missing Open, High, Low, Close, Volume
        }
    )
    with pytest.raises(ValueError, match="missing mandatory columns"):
        data_loader.prepare_records(incomplete_df)


# =====================================================================
# S3 Integration Tests (using Moto)
# =====================================================================

def test_s3_read_silver_parquet_and_upsert_flow(
    data_loader: PostgresDataLoader,
    mock_s3_client: Any,
    sample_silver_dataframe: pd.DataFrame,
    db_session: Session,
) -> None:
    """End-to-end integration test reading Parquet from mocked S3 and loading into database."""
    ticker = "NVDA"
    s3_key = f"{TEST_SILVER_PREFIX}/ticker={ticker}/year=2026/month=03/{ticker}_silver_20260305_120000.parquet"

    # Serialize DataFrame to Parquet buffer and upload to mock S3
    buffer = io.BytesIO()
    sample_silver_dataframe.to_parquet(buffer, index=False, engine="pyarrow")
    mock_s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
    )

    # Read from S3 using data loader
    loaded_df = data_loader.read_silver_parquet(s3_key)
    assert len(loaded_df) == len(sample_silver_dataframe)

    # Prepare and Upsert
    records = data_loader.prepare_records(loaded_df)
    upserted_count = data_loader.upsert_records(records)
    assert upserted_count == 5

    # Verify rows in database
    db_rows = db_session.execute(select(MarketDailyMetrics)).scalars().all()
    assert len(db_rows) == 5
    assert {r.ticker for r in db_rows} == {"NVDA"}


def test_get_latest_silver_key_selection(
    data_loader: PostgresDataLoader,
    mock_s3_client: Any,
) -> None:
    """Verify that get_latest_silver_key picks the most recent chronological file."""
    ticker = "NVDA"
    key_older = f"{TEST_SILVER_PREFIX}/ticker={ticker}/year=2026/month=03/{ticker}_silver_20260301_080000.parquet"
    key_newer = f"{TEST_SILVER_PREFIX}/ticker={ticker}/year=2026/month=03/{ticker}_silver_20260302_143000.parquet"

    mock_s3_client.put_object(Bucket=TEST_BUCKET, Key=key_older, Body=b"fake_parquet_1")
    mock_s3_client.put_object(Bucket=TEST_BUCKET, Key=key_newer, Body=b"fake_parquet_2")

    latest_key = data_loader.get_latest_silver_key(ticker)
    assert latest_key == key_newer


def test_get_latest_silver_key_not_found_raises(
    data_loader: PostgresDataLoader,
) -> None:
    """Verify FileNotFoundError when no Silver files exist for a ticker."""
    with pytest.raises(FileNotFoundError, match="No Silver Parquet files found"):
        data_loader.get_latest_silver_key("NON_EXISTENT_TICKER")


def test_load_latest_ticker_data_end_to_end(
    data_loader: PostgresDataLoader,
    mock_s3_client: Any,
    sample_silver_dataframe: pd.DataFrame,
    db_session: Session,
) -> None:
    """Verify end-to-end load_latest_ticker_data execution."""
    ticker = "NVDA"
    s3_key = f"{TEST_SILVER_PREFIX}/ticker={ticker}/year=2026/month=03/{ticker}_silver_20260305_150000.parquet"

    buffer = io.BytesIO()
    sample_silver_dataframe.to_parquet(buffer, index=False, engine="pyarrow")
    mock_s3_client.put_object(Bucket=TEST_BUCKET, Key=s3_key, Body=buffer.getvalue())

    result = data_loader.load_latest_ticker_data(ticker)
    assert result["status"] == "SUCCESS"
    assert result["ticker"] == ticker
    assert result["records_upserted"] == 5
    assert result["silver_source_key"] == s3_key

    # Confirm rows in database
    db_rows = db_session.execute(select(MarketDailyMetrics)).scalars().all()
    assert len(db_rows) == 5


def test_load_all_target_tickers_summary(
    data_loader: PostgresDataLoader,
    mock_s3_client: Any,
    sample_silver_dataframe: pd.DataFrame,
    db_session: Session,
) -> None:
    """Verify load_all_target_tickers runs across all configured tickers."""
    # Setup files for NVDA and TSM
    for ticker in ["NVDA", "TSM"]:
        df = sample_silver_dataframe.copy()
        df["ticker"] = ticker
        s3_key = f"{TEST_SILVER_PREFIX}/ticker={ticker}/year=2026/month=03/{ticker}_silver_20260305_150000.parquet"
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        mock_s3_client.put_object(Bucket=TEST_BUCKET, Key=s3_key, Body=buffer.getvalue())

    summary = data_loader.load_all_target_tickers()
    assert summary["status"] == "SUCCESS"
    assert "NVDA" in summary["tickers_processed"]
    assert "TSM" in summary["tickers_processed"]
    assert len(summary["failed_tickers"]) == 0

    # Total records in DB: 5 NVDA + 5 TSM = 10 records
    db_rows = db_session.execute(select(MarketDailyMetrics)).scalars().all()
    assert len(db_rows) == 10


# =====================================================================
# Unit Tests: Configuration & Connection URI
# =====================================================================

def test_config_connection_uri_standard() -> None:
    """Verify Postgres connection URI formatting."""
    config = PostgresLoaderConfig(
        db_user="testuser",
        db_password="secretpassword",
        db_host="localhost",
        db_port=5432,
        db_name="test_market_db",
    )
    expected_uri = "postgresql+psycopg2://testuser:secretpassword@localhost:5432/test_market_db"
    assert config.get_connection_uri() == expected_uri


def test_config_connection_uri_from_database_url_normalization() -> None:
    """Verify that postgresql:// prefix is normalized to postgresql+psycopg2://."""
    config = PostgresLoaderConfig(
        database_url="postgresql://usr:pwd@dbhost:5433/custom_db"
    )
    assert config.get_connection_uri() == "postgresql+psycopg2://usr:pwd@dbhost:5433/custom_db"
