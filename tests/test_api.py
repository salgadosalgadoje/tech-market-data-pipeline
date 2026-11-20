"""Automated Unit & Integration Tests for Analytical Consumption API (FastAPI).

Validates REST endpoints using FastAPI TestClient and an in-memory SQLite database,
ensuring 100% isolation from Docker and live PostgreSQL in CI/CD (GitHub Actions).

Tests cover:
- Health probe (`GET /health`) under healthy and failing database scenarios.
- Tickers catalog (`GET /api/v1/tickers`).
- Historical time-series (`GET /api/v1/metrics/{ticker}`) with chronological ordering,
  date range filtering, and limit restrictions.
- Latest metrics query (`GET /api/v1/metrics/{ticker}/latest`).
- Cross-asset comparative market summary (`GET /api/v1/summary`).
- HTTP 404 Not Found error handling for non-existent tickers.
- HTTP 500 Internal Server Error handling for database failures.
"""

from datetime import date, datetime, timedelta, timezone
from typing import Generator
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from src.api.main import app, get_db
from src.load_postgres import Base, MarketDailyMetrics


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def test_engine() -> Generator[Engine, None, None]:
    """Create an isolated in-memory SQLite engine with StaticPool for thread-safe testing."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture
def seed_test_data(test_engine: Engine) -> None:
    """Populate test database with realistic records for NVDA, TSM, and MSFT."""
    session_factory = sessionmaker(bind=test_engine, autocommit=False, autoflush=False)
    session = session_factory()

    base_date = date(2026, 3, 1)

    records = [
        # NVDA (5 days)
        MarketDailyMetrics(
            ticker="NVDA",
            date=base_date + timedelta(days=0),
            open=110.0,
            high=115.0,
            low=109.0,
            close=112.5,
            volume=10000000,
            sma_7=110.0,
            sma_30=105.0,
            daily_return=0.0,
            volatility_7=0.010,
            transformed_at_utc=datetime.now(timezone.utc),
            loaded_at_utc=datetime.now(timezone.utc),
        ),
        MarketDailyMetrics(
            ticker="NVDA",
            date=base_date + timedelta(days=1),
            open=113.0,
            high=118.0,
            low=112.0,
            close=116.0,
            volume=12000000,
            sma_7=114.25,
            sma_30=106.0,
            daily_return=0.0311,
            volatility_7=0.015,
            transformed_at_utc=datetime.now(timezone.utc),
            loaded_at_utc=datetime.now(timezone.utc),
        ),
        MarketDailyMetrics(
            ticker="NVDA",
            date=base_date + timedelta(days=2),
            open=115.5,
            high=119.5,
            low=114.0,
            close=118.0,
            volume=11500000,
            sma_7=115.5,
            sma_30=107.0,
            daily_return=0.0172,
            volatility_7=0.018,
            transformed_at_utc=datetime.now(timezone.utc),
            loaded_at_utc=datetime.now(timezone.utc),
        ),
        MarketDailyMetrics(
            ticker="NVDA",
            date=base_date + timedelta(days=3),
            open=117.0,
            high=121.0,
            low=116.0,
            close=120.5,
            volume=14000000,
            sma_7=116.75,
            sma_30=108.5,
            daily_return=0.0212,
            volatility_7=0.020,
            transformed_at_utc=datetime.now(timezone.utc),
            loaded_at_utc=datetime.now(timezone.utc),
        ),
        MarketDailyMetrics(
            ticker="NVDA",
            date=base_date + timedelta(days=4),
            open=121.0,
            high=125.0,
            low=120.0,
            close=124.0,
            volume=15000000,
            sma_7=118.2,
            sma_30=110.0,
            daily_return=0.0290,
            volatility_7=0.022,
            transformed_at_utc=datetime.now(timezone.utc),
            loaded_at_utc=datetime.now(timezone.utc),
        ),
        # TSM (2 days)
        MarketDailyMetrics(
            ticker="TSM",
            date=base_date + timedelta(days=0),
            open=135.0,
            high=138.0,
            low=134.0,
            close=136.5,
            volume=8000000,
            sma_7=135.0,
            sma_30=130.0,
            daily_return=0.0,
            volatility_7=0.012,
            transformed_at_utc=datetime.now(timezone.utc),
            loaded_at_utc=datetime.now(timezone.utc),
        ),
        MarketDailyMetrics(
            ticker="TSM",
            date=base_date + timedelta(days=1),
            open=137.0,
            high=140.0,
            low=136.0,
            close=139.0,
            volume=9000000,
            sma_7=137.75,
            sma_30=131.0,
            daily_return=0.0183,
            volatility_7=0.014,
            transformed_at_utc=datetime.now(timezone.utc),
            loaded_at_utc=datetime.now(timezone.utc),
        ),
        # MSFT (1 day)
        MarketDailyMetrics(
            ticker="MSFT",
            date=base_date + timedelta(days=0),
            open=420.0,
            high=425.0,
            low=418.0,
            close=422.0,
            volume=20000000,
            sma_7=420.0,
            sma_30=415.0,
            daily_return=0.005,
            volatility_7=0.008,
            transformed_at_utc=datetime.now(timezone.utc),
            loaded_at_utc=datetime.now(timezone.utc),
        ),
    ]

    session.add_all(records)
    session.commit()
    session.close()


@pytest.fixture
def client(test_engine: Engine, seed_test_data: None) -> Generator[TestClient, None, None]:
    """Provide a TestClient with overridden get_db dependency pointing to in-memory SQLite."""
    session_factory = sessionmaker(bind=test_engine, autocommit=False, autoflush=False)

    def _override_get_db() -> Generator[Session, None, None]:
        session = session_factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


# =====================================================================
# Unit & Integration Tests: Health Endpoint
# =====================================================================


def test_health_check_success(client: TestClient) -> None:
    """Verify GET /health returns 200 with healthy status and database connection info."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["database"] == "connected"
    assert "timestamp_utc" in data
    assert data["version"] == "1.0.0"


def test_health_check_db_failure() -> None:
    """Verify GET /health returns 500 when database connectivity fails."""
    def _broken_get_db() -> Generator[Session, None, None]:
        class BrokenSession:
            def execute(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                raise OperationalError("SELECT 1", {}, Exception("Connection refused"))

            def close(self) -> None:
                pass

        yield BrokenSession()  # type: ignore[misc]

    app.dependency_overrides[get_db] = _broken_get_db
    try:
        with TestClient(app) as broken_client:
            response = broken_client.get("/health")
            assert response.status_code == 500
            assert "detail" in response.json()
    finally:
        app.dependency_overrides.clear()


# =====================================================================
# Unit & Integration Tests: Tickers Catalog
# =====================================================================


def test_get_available_tickers(client: TestClient) -> None:
    """Verify GET /api/v1/tickers returns all distinct tickers in sorted order."""
    response = client.get("/api/v1/tickers")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 3
    assert data["tickers"] == ["MSFT", "NVDA", "TSM"]


def test_get_available_tickers_empty_database(test_engine: Engine) -> None:
    """Verify GET /api/v1/tickers returns an empty list when no data is in DB."""
    # Create empty engine without seeded data
    empty_engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(empty_engine)
    empty_factory = sessionmaker(bind=empty_engine)

    def _empty_get_db() -> Generator[Session, None, None]:
        session = empty_factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = _empty_get_db
    try:
        with TestClient(app) as empty_client:
            response = empty_client.get("/api/v1/tickers")
            assert response.status_code == 200
            data = response.json()
            assert data["total"] == 0
            assert data["tickers"] == []
    finally:
        Base.metadata.drop_all(empty_engine)
        empty_engine.dispose()
        app.dependency_overrides.clear()


# =====================================================================
# Unit & Integration Tests: Historical Metrics & Filtering
# =====================================================================


def test_get_ticker_metrics_success_chronological(client: TestClient) -> None:
    """Verify GET /api/v1/metrics/{ticker} returns data ordered chronologically (date ASC)."""
    response = client.get("/api/v1/metrics/NVDA")
    assert response.status_code == 200
    records = response.json()
    assert len(records) == 5

    dates = [r["date"] for r in records]
    assert dates == sorted(dates), "Records must be sorted in ascending chronological order"

    first = records[0]
    assert first["ticker"] == "NVDA"
    assert first["date"] == "2026-03-01"
    assert first["open"] == 110.0
    assert first["close"] == 112.5
    assert first["volume"] == 10000000
    assert first["sma_7"] == 110.0
    assert first["volatility_7"] == 0.010


def test_get_ticker_metrics_case_insensitive(client: TestClient) -> None:
    """Verify ticker query parameter handles lowercase input correctly."""
    response = client.get("/api/v1/metrics/nvda")
    assert response.status_code == 200
    records = response.json()
    assert len(records) == 5
    assert all(r["ticker"] == "NVDA" for r in records)


def test_get_ticker_metrics_not_found_404(client: TestClient) -> None:
    """Verify GET /api/v1/metrics/{ticker} returns 404 for non-existent ticker."""
    response = client.get("/api/v1/metrics/UNKNOWN_TICKER")
    assert response.status_code == 404
    data = response.json()
    assert "not found" in data["detail"].lower()


def test_get_ticker_metrics_filter_by_start_date(client: TestClient) -> None:
    """Verify filtering with start_date parameter."""
    response = client.get("/api/v1/metrics/NVDA?start_date=2026-03-03")
    assert response.status_code == 200
    records = response.json()
    assert len(records) == 3
    assert records[0]["date"] == "2026-03-03"


def test_get_ticker_metrics_filter_by_end_date(client: TestClient) -> None:
    """Verify filtering with end_date parameter."""
    response = client.get("/api/v1/metrics/NVDA?end_date=2026-03-02")
    assert response.status_code == 200
    records = response.json()
    assert len(records) == 2
    assert records[-1]["date"] == "2026-03-02"


def test_get_ticker_metrics_filter_date_range(client: TestClient) -> None:
    """Verify filtering with both start_date and end_date."""
    response = client.get("/api/v1/metrics/NVDA?start_date=2026-03-02&end_date=2026-03-04")
    assert response.status_code == 200
    records = response.json()
    assert len(records) == 3
    dates = [r["date"] for r in records]
    assert dates == ["2026-03-02", "2026-03-03", "2026-03-04"]


def test_get_ticker_metrics_filter_with_limit(client: TestClient) -> None:
    """Verify limit parameter restricts number of returned records."""
    response = client.get("/api/v1/metrics/NVDA?limit=2")
    assert response.status_code == 200
    records = response.json()
    assert len(records) == 2
    assert records[0]["date"] == "2026-03-01"
    assert records[1]["date"] == "2026-03-02"


# =====================================================================
# Unit & Integration Tests: Latest Metrics Endpoint
# =====================================================================


def test_get_latest_ticker_metric_success(client: TestClient) -> None:
    """Verify GET /api/v1/metrics/{ticker}/latest returns the most recent record."""
    response = client.get("/api/v1/metrics/NVDA/latest")
    assert response.status_code == 200
    record = response.json()
    assert record["ticker"] == "NVDA"
    assert record["date"] == "2026-03-05"
    assert record["close"] == 124.0
    assert record["volume"] == 15000000
    assert record["sma_7"] == 118.2
    assert record["volatility_7"] == 0.022


def test_get_latest_ticker_metric_not_found_404(client: TestClient) -> None:
    """Verify GET /api/v1/metrics/{ticker}/latest returns 404 for unknown ticker."""
    response = client.get("/api/v1/metrics/NONEXISTENT/latest")
    assert response.status_code == 404
    data = response.json()
    assert "no metrics found" in data["detail"].lower()


# =====================================================================
# Unit & Integration Tests: Market Summary Endpoint
# =====================================================================


def test_get_market_summary_success(client: TestClient) -> None:
    """Verify GET /api/v1/summary aggregates metrics across all tickers."""
    response = client.get("/api/v1/summary")
    assert response.status_code == 200
    data = response.json()
    assert data["total_tickers"] == 3
    assert "generated_at_utc" in data

    tickers_summary = {item["ticker"]: item for item in data["data"]}
    assert "NVDA" in tickers_summary
    assert "TSM" in tickers_summary
    assert "MSFT" in tickers_summary

    # Verify NVDA summary details
    nvda_sum = tickers_summary["NVDA"]
    assert nvda_sum["latest_date"] == "2026-03-05"
    assert nvda_sum["close"] == 124.0
    assert nvda_sum["volume"] == 15000000
    assert nvda_sum["avg_volatility_7"] is not None
    # NVDA volatility_7 values: [0.010, 0.015, 0.018, 0.020, 0.022] -> mean = 0.017
    assert round(nvda_sum["avg_volatility_7"], 3) == 0.017
