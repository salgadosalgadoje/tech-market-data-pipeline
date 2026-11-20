"""Tech Market Data Pipeline - Analytical Consumption Layer (FastAPI REST API).

Exposes curated financial time-series metrics, technical indicators (SMA, Volatility, Returns),
and cross-sectional summaries from the PostgreSQL Gold Layer (`market_daily_metrics`).

Endpoints:
- GET /health: Health check and PostgreSQL connection verification.
- GET /api/v1/tickers: List of distinct available tickers.
- GET /api/v1/metrics/{ticker}: Historical time-series with date and limit filtering.
- GET /api/v1/metrics/{ticker}/latest: Most recent trading record and technical indicators.
- GET /api/v1/summary: Comparative summary across all assets in the warehouse.
"""

from datetime import date, datetime, timezone
import logging
import os
import sys
from typing import Generator, List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from src.api.schemas import (
    ErrorResponse,
    HealthResponse,
    MarketMetricResponse,
    SummaryResponse,
    TickerListResponse,
    TickerSummaryItem,
)
from src.load_postgres import MarketDailyMetrics, PostgresLoaderConfig

# Module logger setup
logger = logging.getLogger("market_pipeline.api")
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


def create_db_engine() -> Engine:
    """Create a SQLAlchemy Engine with connection pooling for PostgreSQL."""
    config = PostgresLoaderConfig()
    uri = config.get_connection_uri()
    safe_uri = uri.split("@")[-1] if "@" in uri else "database"
    logger.info("Initializing API Database Engine for: %s", safe_uri)
    return create_engine(uri, pool_pre_ping=True)


# Global Engine and Session factory for production
engine: Engine = create_db_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    """Dependency injection to provide a database session per request.

    Yields:
        Session: Active SQLAlchemy database session.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Initialize FastAPI Application
app = FastAPI(
    title="Tech Market Analytics API",
    description=(
        "REST API for the Analytical Consumption Layer (Fase 5) of the Tech Market Data Pipeline. "
        "Serves curated financial series, moving averages (SMA-7, SMA-30), daily returns, and "
        "rolling volatility from the PostgreSQL Gold Layer."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    responses={
        500: {"model": ErrorResponse, "description": "Database or internal server error"},
    },
)

# CORS middleware for local frontend and Streamlit consumption
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(SQLAlchemyError)
def sqlalchemy_exception_handler(_request: Request, exc: SQLAlchemyError) -> JSONResponse:
    """Catch-all handler for SQLAlchemy database exceptions returning HTTP 500."""
    logger.error("Database query failed: %s", exc, exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Database connection error or query execution failure."},
    )


# =====================================================================
# Endpoints
# =====================================================================


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check and DB connectivity probe",
    tags=["Monitoring"],
)
def health_check(db: Session = Depends(get_db)) -> HealthResponse:
    """Verify API operational status and active PostgreSQL database connection.

    Executes a lightweight query (`SELECT 1`) to ensure the Gold Layer is reachable.

    Raises:
        HTTPException: 500 if database connectivity fails.
    """
    try:
        db.execute(text("SELECT 1"))
        return HealthResponse(
            status="healthy",
            database="connected",
            timestamp_utc=datetime.now(timezone.utc),
            version="1.0.0",
        )
    except Exception as exc:
        logger.error("Health check failed - Database unreachable: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database connection failure",
        ) from exc


@app.get(
    "/api/v1/tickers",
    response_model=TickerListResponse,
    summary="List available stock tickers in the Gold Layer",
    tags=["Tickers"],
)
def get_available_tickers(db: Session = Depends(get_db)) -> TickerListResponse:
    """Retrieve all unique ticker symbols currently populated in `market_daily_metrics`."""
    try:
        stmt = (
            select(MarketDailyMetrics.ticker)
            .distinct()
            .order_by(MarketDailyMetrics.ticker.asc())
        )
        tickers = list(db.execute(stmt).scalars().all())
        return TickerListResponse(tickers=tickers, total=len(tickers))
    except SQLAlchemyError as exc:
        logger.error("Failed to fetch available tickers: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving tickers from database.",
        ) from exc


@app.get(
    "/api/v1/metrics/{ticker}",
    response_model=List[MarketMetricResponse],
    summary="Get historical time-series metrics for a ticker",
    tags=["Metrics"],
    responses={
        404: {"model": ErrorResponse, "description": "Ticker not found"},
    },
)
def get_ticker_metrics(
    ticker: str,
    start_date: Optional[date] = Query(
        None,
        description="Filter records starting from this date (inclusive, YYYY-MM-DD).",
    ),
    end_date: Optional[date] = Query(
        None,
        description="Filter records up to this date (inclusive, YYYY-MM-DD).",
    ),
    limit: Optional[int] = Query(
        None,
        ge=1,
        le=5000,
        description="Maximum number of historical records to return.",
    ),
    db: Session = Depends(get_db),
) -> List[MarketDailyMetrics]:
    """Retrieve historical market metrics and technical indicators for a given ticker.

    Records are returned ordered chronologically (`date ASC`).
    Supports optional date filtering and limit constraints.

    Raises:
        HTTPException: 404 if ticker does not exist in Gold Layer.
        HTTPException: 500 on database failure.
    """
    clean_ticker = ticker.strip().upper()

    try:
        # Check if the ticker exists in the Gold Layer
        exists_stmt = (
            select(MarketDailyMetrics.ticker)
            .where(MarketDailyMetrics.ticker == clean_ticker)
            .limit(1)
        )
        if not db.execute(exists_stmt).scalars().first():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Ticker '{clean_ticker}' not found in Gold Layer.",
            )

        # Build chronological query
        stmt = (
            select(MarketDailyMetrics)
            .where(MarketDailyMetrics.ticker == clean_ticker)
        )

        if start_date:
            stmt = stmt.where(MarketDailyMetrics.date >= start_date)
        if end_date:
            stmt = stmt.where(MarketDailyMetrics.date <= end_date)

        stmt = stmt.order_by(MarketDailyMetrics.date.asc())

        if limit:
            stmt = stmt.limit(limit)

        records = list(db.execute(stmt).scalars().all())
        return records
    except HTTPException:
        raise
    except SQLAlchemyError as exc:
        logger.error("Error querying metrics for %s: %s", clean_ticker, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error while querying metrics for ticker '{clean_ticker}'.",
        ) from exc


@app.get(
    "/api/v1/metrics/{ticker}/latest",
    response_model=MarketMetricResponse,
    summary="Get most recent market metrics and indicators for a ticker",
    tags=["Metrics"],
    responses={
        404: {"model": ErrorResponse, "description": "Ticker not found or no metrics available"},
    },
)
def get_latest_ticker_metric(
    ticker: str,
    db: Session = Depends(get_db),
) -> MarketDailyMetrics:
    """Retrieve the single most recent record with all technical indicators for a ticker.

    Raises:
        HTTPException: 404 if ticker has no records in the Gold Layer.
        HTTPException: 500 on database failure.
    """
    clean_ticker = ticker.strip().upper()

    try:
        stmt = (
            select(MarketDailyMetrics)
            .where(MarketDailyMetrics.ticker == clean_ticker)
            .order_by(MarketDailyMetrics.date.desc())
            .limit(1)
        )
        record = db.execute(stmt).scalars().first()

        if not record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No metrics found for ticker '{clean_ticker}' in Gold Layer.",
            )

        return record
    except HTTPException:
        raise
    except SQLAlchemyError as exc:
        logger.error("Error querying latest metric for %s: %s", clean_ticker, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error querying latest metric for ticker '{clean_ticker}'.",
        ) from exc


@app.get(
    "/api/v1/summary",
    response_model=SummaryResponse,
    summary="Comparative financial summary across all tickers",
    tags=["Analytics"],
)
def get_market_summary(db: Session = Depends(get_db)) -> SummaryResponse:
    """Retrieve a cross-sectional comparative summary across all tracked tickers in the Gold Layer.

    Computes for each ticker:
    - Latest closing price (`close`)
    - Latest daily return (`daily_return`)
    - Historical average 7-day rolling volatility (`avg_volatility_7`)
    - Latest traded volume (`volume`)
    - Moving averages (`sma_7`, `sma_30`)

    Raises:
        HTTPException: 500 on database failure.
    """
    try:
        tickers_stmt = (
            select(MarketDailyMetrics.ticker)
            .distinct()
            .order_by(MarketDailyMetrics.ticker.asc())
        )
        tickers = list(db.execute(tickers_stmt).scalars().all())

        summary_data: List[TickerSummaryItem] = []

        for t in tickers:
            latest_stmt = (
                select(MarketDailyMetrics)
                .where(MarketDailyMetrics.ticker == t)
                .order_by(MarketDailyMetrics.date.desc())
                .limit(1)
            )
            latest = db.execute(latest_stmt).scalars().first()
            if not latest:
                continue

            avg_vol_stmt = select(func.avg(MarketDailyMetrics.volatility_7)).where(
                MarketDailyMetrics.ticker == t
            )
            avg_vol = db.execute(avg_vol_stmt).scalar()

            summary_data.append(
                TickerSummaryItem(
                    ticker=t,
                    latest_date=latest.date,
                    close=latest.close,
                    daily_return=latest.daily_return,
                    avg_volatility_7=float(avg_vol) if avg_vol is not None else None,
                    volume=latest.volume,
                    sma_7=latest.sma_7,
                    sma_30=latest.sma_30,
                )
            )

        return SummaryResponse(
            total_tickers=len(summary_data),
            generated_at_utc=datetime.now(timezone.utc),
            data=summary_data,
        )
    except SQLAlchemyError as exc:
        logger.error("Error generating cross-ticker market summary: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database error generating market summary.",
        ) from exc
