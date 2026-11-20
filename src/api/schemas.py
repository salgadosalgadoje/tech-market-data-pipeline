"""Pydantic V2 Response & Request Schemas for the Analytical Consumption Layer API.

Defines strict type models, field validations, and JSON serialization schemas
for endpoints serving curated financial data from the Gold Layer (PostgreSQL).
"""

from datetime import date as dt_date, datetime as dt_datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class MarketMetricResponse(BaseModel):
    """Schema representing daily market metrics and technical indicators for an asset."""

    ticker: str = Field(
        ...,
        description="Stock ticker symbol (e.g., NVDA, TSM, MSFT).",
        examples=["NVDA"],
    )
    date: dt_date = Field(
        ...,
        description="Market trading date (YYYY-MM-DD).",
        examples=["2026-03-05"],
    )
    open: float = Field(
        ...,
        description="Opening price in USD.",
        examples=[120.50],
    )
    high: float = Field(
        ...,
        description="Highest traded price in USD during the session.",
        examples=[125.75],
    )
    low: float = Field(
        ...,
        description="Lowest traded price in USD during the session.",
        examples=[119.80],
    )
    close: float = Field(
        ...,
        description="Closing traded price in USD.",
        examples=[124.20],
    )
    volume: int = Field(
        ...,
        description="Total traded volume (shares).",
        examples=[45000000],
    )
    sma_7: Optional[float] = Field(
        None,
        description="7-day Simple Moving Average on closing price.",
        examples=[122.15],
    )
    sma_30: Optional[float] = Field(
        None,
        description="30-day Simple Moving Average on closing price.",
        examples=[118.90],
    )
    daily_return: Optional[float] = Field(
        None,
        description="Daily percentage return relative to previous session close.",
        examples=[0.0182],
    )
    volatility_7: Optional[float] = Field(
        None,
        description="7-day rolling historical volatility (standard deviation of returns).",
        examples=[0.0215],
    )
    transformed_at_utc: Optional[dt_datetime] = Field(
        None,
        description="Timestamp when the Silver layer processed this record.",
    )
    loaded_at_utc: dt_datetime = Field(
        ...,
        description="Timestamp when record was ingested or updated in the Gold Layer.",
    )

    model_config = ConfigDict(from_attributes=True)


class TickerListResponse(BaseModel):
    """Schema for listing available stock tickers in the Gold Layer."""

    tickers: List[str] = Field(
        ...,
        description="List of distinct stock ticker symbols currently available.",
        examples=[["MSFT", "NVDA", "TSM"]],
    )
    total: int = Field(
        ...,
        description="Total count of available tickers.",
        examples=[3],
    )


class TickerSummaryItem(BaseModel):
    """Comparative financial summary for a single ticker."""

    ticker: str = Field(..., description="Stock ticker symbol.", examples=["NVDA"])
    latest_date: dt_date = Field(..., description="Date of the latest available record.")
    close: float = Field(..., description="Latest closing price in USD.", examples=[124.20])
    daily_return: Optional[float] = Field(
        None,
        description="Latest daily percentage return.",
        examples=[0.0182],
    )
    avg_volatility_7: Optional[float] = Field(
        None,
        description="Average 7-day rolling volatility across historical series.",
        examples=[0.0234],
    )
    volume: int = Field(..., description="Latest session traded volume.", examples=[45000000])
    sma_7: Optional[float] = Field(None, description="Latest 7-day Simple Moving Average.")
    sma_30: Optional[float] = Field(None, description="Latest 30-day Simple Moving Average.")

    model_config = ConfigDict(from_attributes=True)


class SummaryResponse(BaseModel):
    """Comparative financial summary response across all tickers in the Gold Layer."""

    total_tickers: int = Field(..., description="Number of tickers included in summary.")
    generated_at_utc: dt_datetime = Field(..., description="Timestamp of summary generation.")
    data: List[TickerSummaryItem] = Field(
        ...,
        description="Summary metrics per ticker.",
    )


class HealthResponse(BaseModel):
    """API health status and database connectivity response."""

    status: str = Field(..., description="Overall service status ('healthy' or 'degraded').", examples=["healthy"])
    database: str = Field(..., description="PostgreSQL database connection status.", examples=["connected"])
    timestamp_utc: dt_datetime = Field(..., description="Server timestamp in UTC.")
    version: str = Field("1.0.0", description="API version.")


class ErrorResponse(BaseModel):
    """Standardized API error message response."""

    detail: str = Field(..., description="Detailed description of the error.")
