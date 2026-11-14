"""Market Data Warehouse Ingestion & Analytics Layer (Gold Layer).

This module implements the Gold Layer (Analytics Layer) of the Medallion Architecture:
1. Configures database connection to PostgreSQL (or SQLite for testing) via SQLAlchemy.
2. Identifies and downloads the most recent curated Parquet file from the AWS S3 Silver layer (`silver/market_data/`).
3. Defines the ORM data model (`MarketDailyMetrics`) with optimized indexes for time-series analytics.
4. Implements idempotent UPSERT logic (`INSERT ... ON CONFLICT (ticker, date) DO UPDATE`)
   to guarantee pipeline re-runs never duplicate financial data.
5. Ingests cleaned and enriched financial metrics ready for BI, dashboards, and downstream analytics.

Architecture Layer: Medallion Architecture - Gold (Analytics / Warehouse Layer)
"""

from datetime import date, datetime, timezone
import io
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
import numpy as np
import pandas as pd
from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Float,
    Index,
    PrimaryKeyConstraint,
    String,
    create_engine,
    func,
    inspect,
    select,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, sessionmaker

# Load environment variables
load_dotenv()

# Configure module logger
logger = logging.getLogger("market_pipeline.load_postgres")
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

# SQLAlchemy Base Declarative Model
Base = declarative_base()


class MarketDailyMetrics(Base):
    """SQLAlchemy ORM model representing the Gold Layer financial table.

    Stores curated daily market data, technical indicators (SMA-7, SMA-30, daily returns),
    and ingestion audit metadata.

    Composite Primary Key:
        (ticker, date): Ensures a single financial entry per stock ticker and trading day.

    Optimized Indexes:
        - ix_market_daily_metrics_ticker_date: Fast composite lookups for specific asset time ranges.
        - ix_market_daily_metrics_date: Cross-sectional queries across all tickers for a specific date.
        - ix_market_daily_metrics_ticker: Asset-specific filtering and aggregations.
    """

    __tablename__ = "market_daily_metrics"

    ticker = Column(
        String(10),
        primary_key=True,
        nullable=False,
        doc="Stock ticker symbol (e.g., NVDA, TSM, MSFT).",
    )
    date = Column(
        Date,
        primary_key=True,
        nullable=False,
        doc="Market trading calendar date.",
    )
    open = Column(Float, nullable=False, doc="Opening price in USD.")
    high = Column(Float, nullable=False, doc="Highest traded price in USD.")
    low = Column(Float, nullable=False, doc="Lowest traded price in USD.")
    close = Column(Float, nullable=False, doc="Closing traded price in USD.")
    volume = Column(BigInteger, nullable=False, doc="Total trading volume (shares).")
    sma_7 = Column(Float, nullable=True, doc="7-day Simple Moving Average on closing price.")
    sma_30 = Column(Float, nullable=True, doc="30-day Simple Moving Average on closing price.")
    daily_return = Column(Float, nullable=True, doc="Daily percentage return of closing price.")
    volatility_7 = Column(
        Float,
        nullable=True,
        doc="7-day rolling volatility (standard deviation of daily returns).",
    )
    transformed_at_utc = Column(
        DateTime(timezone=True),
        nullable=True,
        doc="Timestamp when the Silver layer processed this record.",
    )
    loaded_at_utc = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        doc="Timestamp when record was ingested/updated in the Gold Layer.",
    )

    __table_args__ = (
        PrimaryKeyConstraint("ticker", "date", name="pk_market_daily_metrics"),
        Index("ix_market_daily_metrics_ticker_date", "ticker", "date"),
        Index("ix_market_daily_metrics_date", "date"),
        Index("ix_market_daily_metrics_ticker", "ticker"),
    )

    def __repr__(self) -> str:
        return (
            f"<MarketDailyMetrics(ticker='{self.ticker}', date='{self.date}', "
            f"close={self.close}, volume={self.volume})>"
        )


class PostgresLoaderConfig:
    """Configuration settings for PostgreSQL database connection and S3 Silver retrieval."""

    def __init__(
        self,
        db_user: Optional[str] = None,
        db_password: Optional[str] = None,
        db_host: Optional[str] = None,
        db_port: Optional[Union[str, int]] = None,
        db_name: Optional[str] = None,
        database_url: Optional[str] = None,
        bucket_name: Optional[str] = None,
        silver_prefix: Optional[str] = None,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        target_tickers: Optional[List[str]] = None,
    ) -> None:
        """Initialize configuration with environment variables or explicit parameters."""
        self.db_user: str = db_user or os.getenv("POSTGRES_USER", "postgres")
        self.db_password: str = db_password or os.getenv("POSTGRES_PASSWORD", "postgres")
        self.db_host: str = db_host or os.getenv("POSTGRES_HOST", "localhost")
        self.db_port: int = int(db_port or os.getenv("POSTGRES_PORT", 5432))
        self.db_name: str = db_name or os.getenv("POSTGRES_DB", "market_data")
        self.database_url: Optional[str] = database_url or os.getenv("DATABASE_URL")

        self.bucket_name: str = bucket_name or os.getenv("S3_BUCKET_NAME", "tech-market-bronze-jsalgado")
        self.silver_prefix: str = (
            silver_prefix or os.getenv("S3_SILVER_PREFIX", "silver/market_data")
        ).strip("/")
        self.region_name: str = region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.aws_access_key_id: Optional[str] = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key: Optional[str] = (
            aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        tickers_env = os.getenv("TARGET_TICKERS", "NVDA,TSM,MSFT")
        self.target_tickers: List[str] = target_tickers or [
            t.strip().upper() for t in tickers_env.split(",") if t.strip()
        ]

    def get_connection_uri(self) -> str:
        """Construct the database connection URI for SQLAlchemy.

        Returns:
            str: Database connection string.
        """
        # If DATABASE_URL is specified and doesn't contain placeholders, use it
        if self.database_url and "your_secure_password_here" not in self.database_url:
            url = self.database_url
            # Normalize dialect for psycopg2 if standard postgresql:// is provided
            if url.startswith("postgresql://"):
                url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
            return url

        # Construct standard postgresql+psycopg2 URI
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


class PostgresDataLoader:
    """Gold Layer Loader responsible for reading Silver Parquets from S3 and Upserting into PostgreSQL."""

    def __init__(
        self,
        config: Optional[PostgresLoaderConfig] = None,
        engine: Optional[Engine] = None,
        s3_client: Optional[Any] = None,
    ) -> None:
        """Initialize loader with config, database engine, and AWS S3 client.

        Args:
            config: Optional PostgresLoaderConfig instance.
            engine: Optional SQLAlchemy Engine (for dependency injection or in-memory SQLite testing).
            s3_client: Optional boto3 S3 client (for dependency injection or moto testing).
        """
        self.config = config or PostgresLoaderConfig()
        self.engine: Engine = engine or self._build_engine()
        self.s3_client = s3_client or self._build_s3_client()
        self.SessionLocal = sessionmaker(bind=self.engine)

    def _build_engine(self) -> Engine:
        """Create a SQLAlchemy Engine with connection pooling and healthcheck."""
        uri = self.config.get_connection_uri()
        # Redact credentials for logging
        safe_uri = uri.split("@")[-1] if "@" in uri else "database"
        logger.info("Initializing SQLAlchemy Engine connecting to: %s", safe_uri)
        return create_engine(uri, pool_pre_ping=True)

    def _build_s3_client(self) -> Any:
        """Create boto3 S3 client with credentials if provided."""
        session_kwargs: Dict[str, Any] = {"region_name": self.config.region_name}
        if self.config.aws_access_key_id and self.config.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = self.config.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key

        return boto3.client("s3", **session_kwargs)

    def create_tables(self) -> None:
        """Create all defined tables and indexes in the target database if they do not exist."""
        logger.info("Verifying and creating database schema for Gold Layer...")
        Base.metadata.create_all(self.engine)
        logger.info("Table '%s' is ready.", MarketDailyMetrics.__tablename__)

    def list_silver_files(self, ticker: Optional[str] = None) -> List[str]:
        """List all Silver Parquet file keys in S3.

        Args:
            ticker: Optional ticker to filter files (e.g. 'NVDA').

        Returns:
            List[str]: S3 object keys sorted chronologically.

        Raises:
            RuntimeError: If AWS S3 list operation fails.
        """
        prefix = (
            f"{self.config.silver_prefix}/ticker={ticker}/"
            if ticker
            else f"{self.config.silver_prefix}/"
        )
        logger.debug("Listing Silver files in s3://%s/%s", self.config.bucket_name, prefix)

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.config.bucket_name, Prefix=prefix)

            keys: List[str] = []
            for page in pages:
                contents = page.get("Contents", [])
                for obj in contents:
                    key = obj.get("Key", "")
                    if key.endswith(".parquet"):
                        keys.append(key)

            keys.sort()
            return keys
        except (ClientError, BotoCoreError) as exc:
            error_msg = f"Failed to list Silver files in bucket '{self.config.bucket_name}' under '{prefix}': {exc}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from exc

    def get_latest_silver_key(self, ticker: str) -> str:
        """Retrieve the latest Silver Parquet S3 key for a given ticker.

        Args:
            ticker: Stock ticker symbol.

        Returns:
            str: S3 object key of the most recent Parquet file.

        Raises:
            FileNotFoundError: If no Parquet files are found for the ticker.
        """
        files = self.list_silver_files(ticker=ticker)
        if not files:
            raise FileNotFoundError(
                f"No Silver Parquet files found for ticker '{ticker}' in s3://{self.config.bucket_name}/{self.config.silver_prefix}/"
            )
        latest_file = files[-1]
        logger.info("Discovered latest Silver Parquet for ticker '%s': %s", ticker, latest_file)
        return latest_file

    def read_silver_parquet(self, s3_key: str) -> pd.DataFrame:
        """Download and deserialize a Silver Parquet file from S3 into a Pandas DataFrame.

        Args:
            s3_key: S3 object key.

        Returns:
            pd.DataFrame: Loaded DataFrame.

        Raises:
            FileNotFoundError: If S3 object does not exist.
            RuntimeError: If S3 download or deserialization fails.
        """
        logger.info("Downloading Silver Parquet from s3://%s/%s", self.config.bucket_name, s3_key)
        try:
            response = self.s3_client.get_object(Bucket=self.config.bucket_name, Key=s3_key)
            body_bytes = response["Body"].read()
            df = pd.read_parquet(io.BytesIO(body_bytes), engine="pyarrow")
            logger.info("Successfully read %d records from s3://%s/%s", len(df), self.config.bucket_name, s3_key)
            return df
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in {"NoSuchKey", "404"}:
                error_msg = f"S3 Silver object not found: s3://{self.config.bucket_name}/{s3_key}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg) from exc
            error_msg = f"AWS S3 ClientError reading s3://{self.config.bucket_name}/{s3_key}: {exc}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from exc
        except Exception as exc:
            error_msg = f"Failed to deserialize Silver Parquet s3://{self.config.bucket_name}/{s3_key}: {exc}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from exc

    def prepare_records(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Normalize DataFrame columns and format records for database ingestion.

        - Maps column names to standardized snake_case.
        - Parses date strings/timestamps into standard datetime.date objects.
        - Converts missing/NaN values into SQL-compatible None.
        - Attaches the warehouse load timestamp (loaded_at_utc).

        Args:
            df: Input DataFrame from Silver layer.

        Returns:
            List[Dict[str, Any]]: Clean list of row dictionaries matching MarketDailyMetrics.

        Raises:
            ValueError: If required columns are missing or DataFrame is empty.
        """
        if df is None or df.empty:
            raise ValueError("Input DataFrame is empty or None. Cannot prepare records.")

        df_norm = df.copy()

        # Rename columns to standard lowercase snake_case
        rename_map = {
            "Date": "date",
            "Ticker": "ticker",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
            "SMA_7": "sma_7",
            "SMA_30": "sma_30",
            "daily_return": "daily_return",
            "Daily_Return": "daily_return",
            "transformed_at_utc": "transformed_at_utc",
        }
        df_norm = df_norm.rename(columns=rename_map)

        # Validate mandatory columns
        mandatory_cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
        missing_cols = [col for col in mandatory_cols if col not in df_norm.columns]
        if missing_cols:
            raise ValueError(f"DataFrame is missing mandatory columns: {missing_cols}")

        # Normalize Date column to datetime.date
        def _parse_date(val: Any) -> date:
            if isinstance(val, (date, datetime)) and not isinstance(val, bool):
                return val.date() if isinstance(val, datetime) else val
            return pd.to_datetime(val).date()

        df_norm["date"] = df_norm["date"].apply(_parse_date)

        # Normalize ticker to uppercase string
        df_norm["ticker"] = df_norm["ticker"].astype(str).str.strip().str.upper()

        # Normalize numeric types
        df_norm["open"] = df_norm["open"].astype(float)
        df_norm["high"] = df_norm["high"].astype(float)
        df_norm["low"] = df_norm["low"].astype(float)
        df_norm["close"] = df_norm["close"].astype(float)
        df_norm["volume"] = df_norm["volume"].astype("int64")

        # Optional technical indicators
        for col in ["sma_7", "sma_30", "daily_return", "volatility_7"]:
            if col in df_norm.columns:
                df_norm[col] = pd.to_numeric(df_norm[col], errors="coerce")
            else:
                df_norm[col] = None

        # Parse transformed_at_utc if present
        if "transformed_at_utc" in df_norm.columns:
            def _parse_ts(val: Any) -> Optional[datetime]:
                if pd.isna(val) or val is None:
                    return None
                if isinstance(val, datetime):
                    return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
                parsed = pd.to_datetime(val)
                return parsed.to_pydatetime() if parsed.tzinfo else parsed.to_pydatetime().replace(tzinfo=timezone.utc)

            df_norm["transformed_at_utc"] = df_norm["transformed_at_utc"].apply(_parse_ts)
        else:
            df_norm["transformed_at_utc"] = None

        # Set loaded_at_utc
        now_utc = datetime.now(timezone.utc)
        df_norm["loaded_at_utc"] = now_utc

        # Replace numpy NaN with Python None for SQL NULL compatibility
        cleaned_records = df_norm.replace({np.nan: None}).to_dict(orient="records")

        # Select only valid table columns
        valid_cols = {col.name for col in MarketDailyMetrics.__table__.columns}
        filtered_records = [
            {k: v for k, v in rec.items() if k in valid_cols}
            for rec in cleaned_records
        ]

        logger.debug("Prepared %d records for database upsert.", len(filtered_records))
        return filtered_records

    def upsert_records(self, records: List[Dict[str, Any]]) -> int:
        """Perform idempotent batch UPSERT into the market_daily_metrics table.

        Uses native `INSERT ... ON CONFLICT (ticker, date) DO UPDATE` for PostgreSQL,
        and dynamically detects SQLite dialect when running under integration tests.

        Idempotency Guarantee:
            If a record with (ticker, date) already exists, it updates all financial metrics
            and the `loaded_at_utc` timestamp, leaving no duplicates.
            If it does not exist, a new row is inserted.

        Args:
            records: Clean list of row dictionaries matching the ORM schema.

        Returns:
            int: Number of records successfully processed.

        Raises:
            ValueError: If records list is empty.
            RuntimeError: If database execution fails.
        """
        if not records:
            logger.warning("Empty records list provided to upsert_records. Skipping execution.")
            return 0

        dialect_name = self.engine.dialect.name
        logger.info(
            "Executing idempotent upsert of %d records (Dialect: %s)...",
            len(records),
            dialect_name,
        )

        table = MarketDailyMetrics.__table__

        # Dynamically import dialect-specific insert
        if dialect_name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as dialect_insert
        else:
            from sqlalchemy.dialects.postgresql import insert as dialect_insert

        insert_stmt = dialect_insert(table).values(records)

        # Build update dictionary mapping excluded columns to table fields
        excluded = insert_stmt.excluded
        update_columns = {
            col.name: getattr(excluded, col.name)
            for col in table.columns
            if col.name not in ["ticker", "date"]
        }

        # Construct statement: ON CONFLICT (ticker, date) DO UPDATE SET ...
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["ticker", "date"],
            set_=update_columns,
        )

        try:
            with self.engine.begin() as connection:
                connection.execute(upsert_stmt)
            logger.info("Successfully upserted %d records into '%s'.", len(records), table.name)
            return len(records)
        except Exception as exc:
            error_msg = f"Database error executing upsert on '{table.name}': {exc}"
            logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg) from exc

    def load_latest_ticker_data(self, ticker: str) -> Dict[str, Any]:
        """Execute end-to-end ingestion pipeline for the most recent Parquet file of a ticker.

        1. Discover the latest Silver Parquet S3 key.
        2. Read and deserialize Parquet from S3.
        3. Prepare and normalize records.
        4. Upsert records into PostgreSQL.

        Args:
            ticker: Stock ticker symbol (e.g., 'NVDA').

        Returns:
            Dict[str, Any]: Execution report for this ticker.
        """
        logger.info("Starting Gold Layer ingestion for ticker: %s", ticker)
        latest_key = self.get_latest_silver_key(ticker)
        df = self.read_silver_parquet(latest_key)
        records = self.prepare_records(df)
        records_upserted = self.upsert_records(records)

        return {
            "ticker": ticker,
            "status": "SUCCESS",
            "silver_source_key": latest_key,
            "records_upserted": records_upserted,
        }

    def load_all_target_tickers(self) -> Dict[str, Any]:
        """Execute Gold Layer ingestion for all configured target tickers.

        Returns:
            Dict[str, Any]: Overall execution report with counts and status per ticker.
        """
        logger.info("==================================================")
        logger.info("🚀 STARTING TECH MARKET GOLD INGESTION PIPELINE")
        logger.info("Target Tickers: %s", self.config.target_tickers)
        logger.info("S3 Bucket: %s", self.config.bucket_name)
        logger.info("Silver Prefix: %s", self.config.silver_prefix)
        logger.info("Target Table: %s", MarketDailyMetrics.__tablename__)
        logger.info("==================================================")

        # Ensure schema exists before ingestion
        self.create_tables()

        summary: Dict[str, Any] = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "SUCCESS",
            "tickers_processed": {},
            "failed_tickers": [],
        }

        for ticker in self.config.target_tickers:
            try:
                result = self.load_latest_ticker_data(ticker)
                summary["tickers_processed"][ticker] = result
            except Exception as exc:
                logger.error("❌ Gold ingestion failed for ticker %s: %s", ticker, exc)
                summary["failed_tickers"].append({"ticker": ticker, "error": str(exc)})
                summary["status"] = "PARTIAL_FAILURE"

        if len(summary["failed_tickers"]) == len(self.config.target_tickers):
            summary["status"] = "FAILURE"

        summary["completed_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("🏁 Gold Layer ingestion completed with status: %s", summary["status"])
        return summary


def main() -> None:
    """Command-line entry point for executing the Gold Layer loader."""
    try:
        loader = PostgresDataLoader()
        report = loader.load_all_target_tickers()
        print("\nGold Layer Ingestion Report:")
        print("----------------------------")
        for ticker, data in report.get("tickers_processed", {}).items():
            print(f"✅ {ticker}: {data['records_upserted']} records upserted from {data['silver_source_key']}")
        for failure in report.get("failed_tickers", []):
            print(f"❌ {failure['ticker']}: {failure['error']}")
    except Exception as exc:
        logger.critical("Fatal Gold pipeline exception: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
