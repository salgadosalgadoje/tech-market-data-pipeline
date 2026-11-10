"""Market Data Extraction & S3 Data Lake Ingestion (Bronze Layer).

This module extracts daily historical market data (e.g., NVIDIA, TSMC, Microsoft)
using Yahoo Finance (yfinance), performs preliminary schema & data sanity checks,
and writes raw immutable artifacts (CSV or Parquet) into an AWS S3 Data Lake (Bronze Layer).

Architecture Layer: Medallion Architecture - Bronze (Raw Ingestion)
"""

import io
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
import pandas as pd
import yfinance as yf

# Load environment variables from .env file if present
load_dotenv()

# Configure logger
logger = logging.getLogger("market_pipeline.extract_s3")
if not logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


class PipelineConfig:
    """Configuration manager for AWS credentials and pipeline parameters."""

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        target_tickers: Optional[List[str]] = None,
        storage_format: Optional[str] = None,
        s3_prefix: Optional[str] = None,
        period: Optional[str] = None,
        interval: Optional[str] = None,
    ) -> None:
        """Initialize pipeline configuration with defaults and environment variables."""
        self.bucket_name: str = bucket_name or os.getenv("S3_BUCKET_NAME", "tech-market-bronze-jsalgado")
        self.region_name: str = region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.aws_access_key_id: Optional[str] = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key: Optional[str] = (
            aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        )

        tickers_env = os.getenv("TARGET_TICKERS", "NVDA,TSM,MSFT")
        self.target_tickers: List[str] = target_tickers or [
            t.strip().upper() for t in tickers_env.split(",") if t.strip()
        ]
        self.storage_format: str = (storage_format or os.getenv("STORAGE_FORMAT", "parquet")).lower()
        if self.storage_format not in {"parquet", "csv"}:
            raise ValueError(f"Unsupported storage format: {self.storage_format}. Must be 'parquet' or 'csv'.")

        self.s3_prefix: str = (s3_prefix or os.getenv("S3_BRONZE_PREFIX", "bronze/market_data")).strip("/")
        self.period: str = period or os.getenv("EXTRACTION_PERIOD", "1mo")
        self.interval: str = interval or os.getenv("EXTRACTION_INTERVAL", "1d")

    def validate_for_s3(self) -> None:
        """Validate that the bucket name is specified."""
        if not self.bucket_name:
            raise ValueError("S3_BUCKET_NAME is not set. Please provide a valid S3 bucket name.")


class MarketDataExtractor:
    """Responsible for fetching and lightly validating market data from yfinance."""

    EXPECTED_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]

    def __init__(self, period: str = "1mo", interval: str = "1d") -> None:
        """Initialize the extractor with target period and interval.

        Args:
            period: Duration to download (e.g., '1mo', '3mo', '1y').
            interval: Frequency of datapoints (e.g., '1d', '1h').
        """
        self.period = period
        self.interval = interval

    def fetch_ticker_data(self, ticker: str) -> pd.DataFrame:
        """Fetch historical market data for a given ticker symbol.

        Args:
            ticker: Stock symbol (e.g. 'NVDA', 'TSM', 'MSFT').

        Returns:
            pd.DataFrame: Raw market data with standard financial columns.

        Raises:
            ValueError: If no data was returned or data validation fails.
            RuntimeError: If yfinance encounters a network/API failure.
        """
        logger.info(
            "Starting extraction for ticker: %s (Period: %s, Interval: %s)",
            ticker,
            self.period,
            self.interval,
        )

        try:
            stock = yf.Ticker(ticker)
            df: pd.DataFrame = stock.history(period=self.period, interval=self.interval)
        except Exception as exc:
            error_msg = f"Failed to fetch market data from yfinance for {ticker}: {exc}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from exc

        # Preliminary sanity validation (Bronze Layer checks)
        self.validate_extracted_data(df, ticker)

        # Standardize DataFrame for Bronze storage
        df = df.copy()
        df = df.reset_index()

        # Ensure Date/Datetime column is standardized as a string or timestamp
        date_col = "Date" if "Date" in df.columns else "Datetime"
        if date_col in df.columns:
            df["Date"] = pd.to_datetime(df[date_col]).dt.tz_localize(None)
            if date_col == "Datetime":
                df = df.drop(columns=["Datetime"])

        # Add ingestion metadata
        df["ticker"] = ticker
        df["ingested_at_utc"] = datetime.now(timezone.utc).isoformat()

        logger.info("Successfully fetched %d records for %s", len(df), ticker)
        return df

    def validate_extracted_data(self, df: pd.DataFrame, ticker: str) -> None:
        """Perform preliminary validation on the downloaded dataset.

        Args:
            df: Downloaded Pandas DataFrame.
            ticker: Stock symbol under evaluation.

        Raises:
            ValueError: If dataframe is empty or lacks required financial indicators.
        """
        if df is None or df.empty:
            raise ValueError(f"Extracted dataset for ticker '{ticker}' is empty.")

        missing_cols = [col for col in self.EXPECTED_COLUMNS if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"Extracted data for '{ticker}' missing expected columns: {missing_cols}"
            )

        # Ensure we have at least 1 valid row of data
        if len(df) == 0:
            raise ValueError(f"Extracted dataset for ticker '{ticker}' contains 0 records.")


class S3DataLakeService:
    """Manages S3 bucket interactions and data uploads with credentials and error handling."""

    def __init__(self, config: PipelineConfig, s3_client=None) -> None:
        """Initialize the S3 service.

        Args:
            config: Pipeline configuration object.
            s3_client: Optional boto3 s3 client (injected during unit tests or mock runs).
        """
        self.config = config
        self.s3_client = s3_client or self._build_s3_client()

    def _build_s3_client(self):
        """Build and configure the boto3 S3 client using explicit or environment credentials."""
        session_kwargs = {"region_name": self.config.region_name}
        if self.config.aws_access_key_id and self.config.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = self.config.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key

        return boto3.client("s3", **session_kwargs)

    def check_bucket_exists(self, bucket_name: str) -> bool:
        """Check if an S3 bucket exists and is accessible.

        Args:
            bucket_name: Name of the bucket.

        Returns:
            bool: True if bucket exists and accessible, False otherwise.
        """
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            logger.warning(
                "Bucket '%s' check failed (Code: %s): %s",
                bucket_name,
                error_code,
                exc,
            )
            return False
        except BotoCoreError as exc:
            logger.error("BotoCore error connecting to S3: %s", exc)
            return False

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        ticker: str,
        storage_format: Optional[str] = None,
    ) -> str:
        """Serialize and upload a DataFrame to the S3 Bronze Data Lake.

        Hive-partitioned S3 key pattern:
        {prefix}/ticker={ticker}/year={YYYY}/month={MM}/{ticker}_{YYYYMMDD_HHMMSS}.{ext}

        Args:
            df: DataFrame to serialize and upload.
            ticker: Stock symbol.
            storage_format: Format ('parquet' or 'csv'). Defaults to config.

        Returns:
            str: The full S3 URI where the artifact was stored (s3://bucket/key).

        Raises:
            RuntimeError: If upload fails due to AWS errors or serialization issues.
        """
        fmt = (storage_format or self.config.storage_format).lower()
        now = datetime.now(timezone.utc)
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")
        year_str = now.strftime("%Y")
        month_str = now.strftime("%m")

        # Partitioned key path for Data Lake best practices
        s3_key = (
            f"{self.config.s3_prefix}/ticker={ticker}/year={year_str}/"
            f"month={month_str}/{ticker}_{timestamp_str}.{fmt}"
        )

        buffer = io.BytesIO()
        try:
            if fmt == "parquet":
                df.to_parquet(buffer, index=False, engine="pyarrow")
                content_type = "application/vnd.apache.parquet"
            elif fmt == "csv":
                df.to_csv(buffer, index=False, encoding="utf-8")
                content_type = "text/csv"
            else:
                raise ValueError(f"Unsupported format: {fmt}")

            buffer.seek(0)
            file_size_bytes = buffer.getbuffer().nbytes

            logger.info(
                "Uploading %s artifact to s3://%s/%s (Size: %.2f KB)",
                fmt.upper(),
                self.config.bucket_name,
                s3_key,
                file_size_bytes / 1024,
            )

            self.s3_client.put_object(
                Bucket=self.config.bucket_name,
                Key=s3_key,
                Body=buffer.getvalue(),
                ContentType=content_type,
                Metadata={
                    "ticker": ticker,
                    "record_count": str(len(df)),
                    "pipeline_stage": "bronze_raw",
                    "ingestion_utc": now.isoformat(),
                },
            )

            s3_uri = f"s3://{self.config.bucket_name}/{s3_key}"
            logger.info("Successfully uploaded artifact to %s", s3_uri)
            return s3_uri

        except (ClientError, BotoCoreError) as exc:
            error_msg = f"AWS S3 upload error for {ticker} at {s3_key}: {exc}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from exc
        except Exception as exc:
            error_msg = f"Unexpected error during data serialization/upload for {ticker}: {exc}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from exc


class TechMarketExtractionPipeline:
    """Orchestrator for the Phase 1 Raw Ingestion Pipeline (Bronze Layer)."""

    def __init__(
        self,
        config: Optional[PipelineConfig] = None,
        extractor: Optional[MarketDataExtractor] = None,
        s3_service: Optional[S3DataLakeService] = None,
    ) -> None:
        """Initialize the extraction pipeline orchestrator.

        Args:
            config: Pipeline configuration.
            extractor: Instance of MarketDataExtractor.
            s3_service: Instance of S3DataLakeService.
        """
        self.config = config or PipelineConfig()
        self.extractor = extractor or MarketDataExtractor(
            period=self.config.period,
            interval=self.config.interval,
        )
        self.s3_service = s3_service or S3DataLakeService(self.config)

    def run(self) -> Dict[str, Any]:
        """Execute the extraction and ingestion pipeline for all configured tickers.

        Returns:
            Dict[str, Any]: Summary report containing status, records extracted, and S3 URIs.
        """
        logger.info("==================================================")
        logger.info("🚀 STARTING TECH MARKET EXTRACTION PIPELINE (BRONZE)")
        logger.info("Target Tickers: %s", self.config.target_tickers)
        logger.info("S3 Target Bucket: %s", self.config.bucket_name)
        logger.info("Storage Format: %s", self.config.storage_format)
        logger.info("==================================================")

        summary: Dict[str, Any] = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "SUCCESS",
            "tickers_processed": {},
            "failed_tickers": [],
        }

        for ticker in self.config.target_tickers:
            try:
                # Step 1: Extraction & Validation
                df = self.extractor.fetch_ticker_data(ticker)

                # Step 2: Upload to S3 Data Lake (Bronze)
                s3_uri = self.s3_service.upload_dataframe(df, ticker)

                summary["tickers_processed"][ticker] = {
                    "records": len(df),
                    "s3_uri": s3_uri,
                    "status": "SUCCESS",
                }
            except Exception as exc:
                logger.error("❌ Pipeline failed for ticker %s: %s", ticker, exc)
                summary["failed_tickers"].append({"ticker": ticker, "error": str(exc)})
                summary["status"] = "PARTIAL_FAILURE"

        if len(summary["failed_tickers"]) == len(self.config.target_tickers):
            summary["status"] = "FAILURE"

        summary["completed_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("🏁 Extraction pipeline completed with status: %s", summary["status"])
        return summary


def main() -> None:
    """Entry point for command line execution."""
    try:
        pipeline = TechMarketExtractionPipeline()
        report = pipeline.run()
        print("\nPipeline Execution Report:")
        print("-------------------------")
        for ticker, data in report.get("tickers_processed", {}).items():
            print(f"✅ {ticker}: {data['records']} records -> {data['s3_uri']}")
        for failure in report.get("failed_tickers", []):
            print(f"❌ {failure['ticker']}: {failure['error']}")
    except Exception as exc:
        logger.critical("Fatal pipeline exception: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
