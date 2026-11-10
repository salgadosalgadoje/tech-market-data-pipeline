"""Market Data Transformation & Silver Layer Curation.

This module is responsible for the Silver Layer (Curated Layer) in the Medallion Architecture:
1. Reading raw Parquet files from the AWS S3 Bronze Data Lake (`bronze/market_data/ticker=.../`).
2. Validating schema consistency and data sanity using Pydantic (rejecting negative prices, negative volumes, and future dates).
3. Data cleansing and enrichment using Pandas:
   - Handling missing values via forward-fill (ffill) and sensible dropna policies.
   - Calculating 7-day and 30-day Simple Moving Averages (SMA-7, SMA-30) on closing prices.
   - Calculating daily percentage returns.
4. Serializing and writing curated datasets back to AWS S3 in Parquet format (`silver/market_data/ticker=.../`).

Architecture Layer: Medallion Architecture - Silver (Curated / Cleaned Data)
"""

from datetime import date, datetime, timezone
import io
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Load environment variables from .env
load_dotenv()

# Configure module logger
logger = logging.getLogger("market_pipeline.transform")
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


class MarketRecordSchema(BaseModel):
    """Pydantic model representing a strictly validated financial market record.

    Enforces data integrity by rejecting:
    - Non-positive prices (Open, High, Low, Close must be > 0).
    - Inconsistent price bounds (High must be >= Low).
    - Negative trading volumes (Volume must be >= 0).
    - Future dates (Date cannot be later than current UTC date).
    """

    model_config = ConfigDict(extra="ignore")

    Date: Any
    ticker: str = Field(min_length=1, description="Stock ticker symbol (e.g. NVDA, TSM, MSFT)")
    Open: float = Field(gt=0, description="Opening price must be strictly positive")
    High: float = Field(gt=0, description="Highest price must be strictly positive")
    Low: float = Field(gt=0, description="Lowest price must be strictly positive")
    Close: float = Field(gt=0, description="Closing price must be strictly positive")
    Volume: int = Field(ge=0, description="Trading volume must be non-negative")

    @field_validator("Date")
    @classmethod
    def validate_date_not_in_future(cls, value: Any) -> Any:
        """Validate that the record date does not refer to a future date.

        Args:
            value: Date string (YYYY-MM-DD), date object, or datetime object.

        Returns:
            Any: Validated date value.

        Raises:
            ValueError: If the date cannot be parsed or is in the future.
        """
        parsed_date: date
        try:
            if isinstance(value, str):
                parsed_date = pd.to_datetime(value).date()
            elif isinstance(value, datetime):
                parsed_date = value.date()
            elif isinstance(value, date):
                parsed_date = value
            else:
                raise ValueError(f"Unsupported date format or type: {type(value)}")
        except Exception as exc:
            raise ValueError(f"Failed to parse date '{value}': {exc}") from exc

        today_utc = datetime.now(timezone.utc).date()
        if parsed_date > today_utc:
            raise ValueError(
                f"Record date '{parsed_date}' cannot be in the future (Current UTC: {today_utc})."
            )
        return value

    @model_validator(mode="after")
    def validate_price_consistency(self) -> "MarketRecordSchema":
        """Validate that high price is not lower than low price."""
        if self.High < self.Low:
            raise ValueError(
                f"Inconsistent price bounds: High ({self.High}) cannot be less than Low ({self.Low})."
            )
        return self


class SilverPipelineConfig:
    """Configuration holder for Silver Layer processing and S3 paths."""

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        region_name: Optional[str] = None,
        bronze_prefix: Optional[str] = None,
        silver_prefix: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        target_tickers: Optional[List[str]] = None,
    ) -> None:
        """Initialize pipeline configuration parameters.

        Args:
            bucket_name: Name of the AWS S3 Data Lake bucket.
            region_name: AWS region identifier (e.g. 'us-east-1').
            bronze_prefix: S3 path prefix for raw Bronze data.
            silver_prefix: S3 path prefix for transformed Silver data.
            aws_access_key_id: AWS Access Key ID.
            aws_secret_access_key: AWS Secret Access Key.
            target_tickers: List of stock symbols to process.
        """
        self.bucket_name: str = bucket_name or os.getenv("S3_BUCKET_NAME", "tech-market-bronze-jsalgado")
        self.region_name: str = region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.bronze_prefix: str = (
            bronze_prefix or os.getenv("S3_BRONZE_PREFIX", "bronze/market_data")
        ).strip("/")
        self.silver_prefix: str = (
            silver_prefix or os.getenv("S3_SILVER_PREFIX", "silver/market_data")
        ).strip("/")
        self.aws_access_key_id: Optional[str] = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key: Optional[str] = (
            aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        tickers_env = os.getenv("TARGET_TICKERS", "NVDA,TSM,MSFT")
        self.target_tickers: List[str] = target_tickers or [
            t.strip().upper() for t in tickers_env.split(",") if t.strip()
        ]


class MarketDataTransformer:
    """Performs transformation, schema validation, technical calculations, and S3 I/O for the Silver Layer."""

    def __init__(
        self,
        config: Optional[SilverPipelineConfig] = None,
        s3_client: Optional[Any] = None,
    ) -> None:
        """Initialize the transformer with configuration and AWS S3 client.

        Args:
            config: Optional SilverPipelineConfig object.
            s3_client: Optional boto3 S3 client (injected for testing/mocking).
        """
        self.config = config or SilverPipelineConfig()
        self.s3_client = s3_client or self._build_s3_client()

    def _build_s3_client(self) -> Any:
        """Create a boto3 S3 client using environment or explicit credentials."""
        session_kwargs: Dict[str, Any] = {"region_name": self.config.region_name}
        if self.config.aws_access_key_id and self.config.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = self.config.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key

        return boto3.client("s3", **session_kwargs)

    def list_bronze_files(self, ticker: str) -> List[str]:
        """List all Bronze Parquet file keys in S3 for a given ticker.

        Args:
            ticker: Stock symbol (e.g., 'NVDA').

        Returns:
            List[str]: S3 keys for the ticker's Bronze files, sorted chronologically.

        Raises:
            RuntimeError: If AWS S3 list operation fails.
        """
        prefix = f"{self.config.bronze_prefix}/ticker={ticker}/"
        logger.debug("Listing Bronze files in s3://%s/%s", self.config.bucket_name, prefix)

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
            error_msg = f"Failed to list Bronze files for ticker '{ticker}' in bucket '{self.config.bucket_name}': {exc}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from exc

    def get_latest_bronze_key(self, ticker: str) -> str:
        """Find the latest Parquet file key in Bronze layer for a given ticker.

        Args:
            ticker: Stock symbol.

        Returns:
            str: S3 key of the most recent Parquet file.

        Raises:
            FileNotFoundError: If no Parquet files exist for the given ticker.
        """
        files = self.list_bronze_files(ticker)
        if not files:
            raise FileNotFoundError(
                f"No Bronze Parquet files found for ticker '{ticker}' under prefix '{self.config.bronze_prefix}/ticker={ticker}/'."
            )
        return files[-1]

    def read_bronze_parquet(self, s3_key: str) -> pd.DataFrame:
        """Read a Parquet file from S3 Bronze layer into a Pandas DataFrame.

        Args:
            s3_key: Path of the S3 object key.

        Returns:
            pd.DataFrame: Loaded DataFrame.

        Raises:
            FileNotFoundError: If S3 object does not exist.
            RuntimeError: If S3 download or Parquet deserialization fails.
        """
        logger.info("Reading Bronze Parquet from s3://%s/%s", self.config.bucket_name, s3_key)
        try:
            response = self.s3_client.get_object(Bucket=self.config.bucket_name, Key=s3_key)
            body_bytes = response["Body"].read()
            df = pd.read_parquet(io.BytesIO(body_bytes), engine="pyarrow")
            logger.info("Successfully read %d records from s3://%s/%s", len(df), self.config.bucket_name, s3_key)
            return df
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in {"NoSuchKey", "404"}:
                error_msg = f"S3 Bronze object not found: s3://{self.config.bucket_name}/{s3_key}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg) from exc
            error_msg = f"AWS S3 ClientError reading s3://{self.config.bucket_name}/{s3_key}: {exc}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from exc
        except Exception as exc:
            error_msg = f"Failed to deserialize Parquet from s3://{self.config.bucket_name}/{s3_key}: {exc}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from exc

    def validate_records(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate every record in the DataFrame using MarketRecordSchema (Pydantic).

        Args:
            df: Raw or cleaned DataFrame containing market records.

        Returns:
            pd.DataFrame: Original DataFrame if all rows pass validation.

        Raises:
            ValueError: If the DataFrame is empty.
            ValidationError: If any record violates schema constraints.
        """
        if df is None or df.empty:
            raise ValueError("Cannot validate empty or None DataFrame.")

        logger.info("Validating %d records against MarketRecordSchema...", len(df))
        records: List[Dict[str, Any]] = df.to_dict(orient="records")
        for idx, record in enumerate(records):
            try:
                MarketRecordSchema(**record)
            except Exception as exc:
                logger.error("Data validation failure at row index %d: %s", idx, exc)
                raise

        logger.info("All %d records passed Pydantic schema validation successfully.", len(df))
        return df

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean dataset by handling null values and normalizing types/order.

        Applies forward-fill (`ffill`) followed by backward-fill (`bfill`)
        on numerical price columns, drops unrecoverable rows, and sorts chronologically.

        Args:
            df: Input DataFrame.

        Returns:
            pd.DataFrame: Cleaned and normalized DataFrame.

        Raises:
            ValueError: If DataFrame is empty or lacks required columns.
        """
        if df is None or df.empty:
            raise ValueError("Input DataFrame for cleaning is empty.")

        cleaned_df = df.copy()

        # Ensure Date column is in datetime format and sort chronologically
        if "Date" in cleaned_df.columns:
            cleaned_df["Date"] = pd.to_datetime(cleaned_df["Date"])
            cleaned_df = cleaned_df.sort_values(by="Date", ascending=True).reset_index(drop=True)

        price_cols = ["Open", "High", "Low", "Close"]
        numeric_cols = [c for c in price_cols if c in cleaned_df.columns]

        # Impute missing values: Forward fill first, then backward fill for edge cases
        cleaned_df[numeric_cols] = cleaned_df[numeric_cols].ffill().bfill()

        # If Volume is present, forward fill then fill remaining with 0
        if "Volume" in cleaned_df.columns:
            cleaned_df["Volume"] = cleaned_df["Volume"].ffill().fillna(0).astype("int64")

        # Drop any remaining unrecoverable null rows in mandatory fields
        cleaned_df = cleaned_df.dropna(subset=numeric_cols).reset_index(drop=True)

        logger.info("Data cleaning completed. Retained %d clean records.", len(cleaned_df))
        return cleaned_df

    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators on closing price (SMA-7, SMA-30, daily return).

        Indicators calculated:
        - SMA_7: Simple Moving Average over a 7-day window.
        - SMA_30: Simple Moving Average over a 30-day window.
        - daily_return: Daily percentage change in closing price ((Close_t - Close_{t-1}) / Close_{t-1}).

        Args:
            df: Cleaned DataFrame with 'Close' and 'Date' columns.

        Returns:
            pd.DataFrame: Enriched DataFrame with technical indicator columns.

        Raises:
            ValueError: If 'Close' column is missing or DataFrame is empty.
        """
        if df is None or df.empty:
            raise ValueError("Cannot calculate indicators on empty DataFrame.")
        if "Close" not in df.columns:
            raise ValueError("Column 'Close' is missing from DataFrame.")

        enriched_df = df.copy()

        # Calculate Simple Moving Averages with min_periods=1 to support initial days and monthly datasets
        enriched_df["SMA_7"] = enriched_df["Close"].rolling(window=7, min_periods=1).mean().round(4)
        enriched_df["SMA_30"] = enriched_df["Close"].rolling(window=30, min_periods=1).mean().round(4)

        # Calculate daily percentage return: pct_change()
        # The first day will be NaN; we fill it with 0.0 (no return on day 0)
        enriched_df["daily_return"] = (
            enriched_df["Close"].pct_change().fillna(0.0).round(6)
        )

        logger.info(
            "Calculated SMA_7, SMA_30, and daily_return for %d records.",
            len(enriched_df),
        )
        return enriched_df

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Complete in-memory transformation pipeline for a market DataFrame.

        Steps:
        1. Clean data (impute nulls, sort dates).
        2. Validate records against Pydantic schema.
        3. Compute technical indicators (SMA-7, SMA-30, daily_return).
        4. Attach transformation audit timestamp.

        Args:
            df: Raw DataFrame from Bronze layer.

        Returns:
            pd.DataFrame: Curated Silver-ready DataFrame.
        """
        cleaned_df = self.clean_data(df)
        self.validate_records(cleaned_df)
        transformed_df = self.calculate_technical_indicators(cleaned_df)

        transformed_df["transformed_at_utc"] = datetime.now(timezone.utc).isoformat()
        return transformed_df

    def write_silver_parquet(self, df: pd.DataFrame, ticker: str) -> str:
        """Serialize and upload the curated Silver DataFrame to S3 as Parquet.

        Partitioned S3 key path:
        {silver_prefix}/ticker={ticker}/year={YYYY}/month={MM}/{ticker}_silver_{YYYYMMDD_HHMMSS}.parquet

        Args:
            df: Curated DataFrame to upload.
            ticker: Stock symbol.

        Returns:
            str: Full S3 URI of the uploaded Silver artifact (s3://bucket/key).

        Raises:
            RuntimeError: If S3 upload or Parquet serialization fails.
        """
        now = datetime.now(timezone.utc)
        year_str = now.strftime("%Y")
        month_str = now.strftime("%m")
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")

        s3_key = (
            f"{self.config.silver_prefix}/ticker={ticker}/year={year_str}/"
            f"month={month_str}/{ticker}_silver_{timestamp_str}.parquet"
        )

        buffer = io.BytesIO()
        try:
            # Format Date column back to ISO string or clean timestamp for Parquet storage
            upload_df = df.copy()
            if "Date" in upload_df.columns:
                upload_df["Date"] = upload_df["Date"].astype(str)

            upload_df.to_parquet(buffer, index=False, engine="pyarrow")
            buffer.seek(0)
            file_size_bytes = buffer.getbuffer().nbytes

            logger.info(
                "Uploading Silver Parquet to s3://%s/%s (Size: %.2f KB, Records: %d)",
                self.config.bucket_name,
                s3_key,
                file_size_bytes / 1024,
                len(upload_df),
            )

            self.s3_client.put_object(
                Bucket=self.config.bucket_name,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType="application/vnd.apache.parquet",
                Metadata={
                    "ticker": ticker,
                    "record_count": str(len(upload_df)),
                    "pipeline_stage": "silver_curated",
                    "transformed_at_utc": now.isoformat(),
                },
            )

            s3_uri = f"s3://{self.config.bucket_name}/{s3_key}"
            logger.info("Successfully uploaded Silver Parquet to %s", s3_uri)
            return s3_uri

        except (ClientError, BotoCoreError) as exc:
            error_msg = f"AWS S3 upload error for Silver ticker {ticker} at {s3_key}: {exc}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from exc
        except Exception as exc:
            error_msg = f"Unexpected error uploading Silver artifact for {ticker}: {exc}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from exc

    def process_ticker(self, ticker: str) -> Dict[str, Any]:
        """Execute end-to-end Silver Layer pipeline for a specific ticker.

        1. Discover latest Bronze Parquet file in S3.
        2. Read Bronze data into memory.
        3. Transform, clean, validate and enrich.
        4. Write curated Parquet into Silver layer in S3.

        Args:
            ticker: Stock symbol (e.g. 'NVDA').

        Returns:
            Dict[str, Any]: Execution summary for the ticker.
        """
        logger.info("Processing Silver Layer transformation for ticker: %s", ticker)
        latest_bronze_key = self.get_latest_bronze_key(ticker)
        raw_df = self.read_bronze_parquet(latest_bronze_key)

        transformed_df = self.transform_dataframe(raw_df)
        silver_uri = self.write_silver_parquet(transformed_df, ticker)

        return {
            "ticker": ticker,
            "status": "SUCCESS",
            "bronze_source_key": latest_bronze_key,
            "silver_s3_uri": silver_uri,
            "records_processed": len(transformed_df),
        }

    def run(self) -> Dict[str, Any]:
        """Execute Silver Layer transformation across all target tickers.

        Returns:
            Dict[str, Any]: Overall execution report.
        """
        logger.info("==================================================")
        logger.info("🚀 STARTING TECH MARKET TRANSFORMATION PIPELINE (SILVER)")
        logger.info("Target Tickers: %s", self.config.target_tickers)
        logger.info("S3 Bucket: %s", self.config.bucket_name)
        logger.info("Bronze Prefix: %s", self.config.bronze_prefix)
        logger.info("Silver Prefix: %s", self.config.silver_prefix)
        logger.info("==================================================")

        summary: Dict[str, Any] = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "SUCCESS",
            "tickers_processed": {},
            "failed_tickers": [],
        }

        for ticker in self.config.target_tickers:
            try:
                result = self.process_ticker(ticker)
                summary["tickers_processed"][ticker] = result
            except Exception as exc:
                logger.error("❌ Silver pipeline failed for ticker %s: %s", ticker, exc)
                summary["failed_tickers"].append({"ticker": ticker, "error": str(exc)})
                summary["status"] = "PARTIAL_FAILURE"

        if len(summary["failed_tickers"]) == len(self.config.target_tickers):
            summary["status"] = "FAILURE"

        summary["completed_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("🏁 Silver transformation pipeline completed with status: %s", summary["status"])
        return summary


def main() -> None:
    """Entry point for command line execution."""
    try:
        transformer = MarketDataTransformer()
        report = transformer.run()
        print("\nSilver Pipeline Execution Report:")
        print("---------------------------------")
        for ticker, data in report.get("tickers_processed", {}).items():
            print(f"✅ {ticker}: {data['records_processed']} records -> {data['silver_s3_uri']}")
        for failure in report.get("failed_tickers", []):
            print(f"❌ {failure['ticker']}: {failure['error']}")
    except Exception as exc:
        logger.critical("Fatal Silver pipeline exception: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
