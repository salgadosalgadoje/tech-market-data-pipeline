"""Unit and integration tests for data transformation and validation logic (Silver Layer).

Tests the Medallion Architecture Phase 2 Silver Layer implementation:
- Mathematical accuracy of technical indicator calculations (SMA-7, SMA-30, daily return).
- Pydantic schema validation (MarketRecordSchema) rejecting invalid/corrupt data.
- Data cleaning and null imputation logic.
- S3 read/write integration using moto to emulate AWS S3 locally without real AWS credentials.
"""

from datetime import date, datetime, timedelta, timezone
import io
from typing import Any, Generator
from unittest.mock import MagicMock, patch

import boto3
import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

try:
    from moto import mock_aws
except ImportError:
    from moto import mock_s3 as mock_aws

from src.transform import (
    MarketDataTransformer,
    MarketRecordSchema,
    SilverPipelineConfig,
)

# Test Constants
TEST_BUCKET_NAME = "test-market-data-lake-silver"
TEST_REGION = "us-east-1"

# Dynamically computed dates relative to today's UTC date to guarantee consistency across environments
TODAY_UTC = datetime.now(timezone.utc).date()
VALID_PAST_DATE_STR = (TODAY_UTC - timedelta(days=5)).strftime("%Y-%m-%d")
FUTURE_DATE_STR = (TODAY_UTC + timedelta(days=30)).strftime("%Y-%m-%d")


@pytest.fixture(autouse=True)
def aws_credentials_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mocked AWS credentials for moto test isolation."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing_access_key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing_secret_key")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing_security_token")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing_session_token")
    monkeypatch.setenv("AWS_DEFAULT_REGION", TEST_REGION)
    monkeypatch.setenv("S3_BUCKET_NAME", TEST_BUCKET_NAME)


@pytest.fixture
def sample_raw_dataframe() -> pd.DataFrame:
    """Fixture providing a mock 10-day raw DataFrame with past dates as extracted from Bronze layer."""
    start_date = TODAY_UTC - timedelta(days=15)
    dates = pd.date_range(start=start_date, periods=10, freq="D")
    data = {
        "Date": [d.strftime("%Y-%m-%d") for d in dates],
        "ticker": ["NVDA"] * 10,
        "Open": [100.0 + i for i in range(10)],
        "High": [105.0 + i for i in range(10)],
        "Low": [98.0 + i for i in range(10)],
        "Close": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0],
        "Volume": [1000000 + (i * 10000) for i in range(10)],
        "ingested_at_utc": [datetime.now(timezone.utc).isoformat()] * 10,
    }
    return pd.DataFrame(data)


@pytest.fixture
def mock_s3() -> Generator[Any, None, None]:
    """Moto fixture to create a clean mocked S3 bucket."""
    with mock_aws():
        s3 = boto3.client("s3", region_name=TEST_REGION)
        s3.create_bucket(Bucket=TEST_BUCKET_NAME)
        yield s3


# =====================================================================
# 1. Tests for MarketRecordSchema (Pydantic Validation)
# =====================================================================
class TestMarketRecordSchema:
    """Test suite for MarketRecordSchema Pydantic validation."""

    def test_valid_market_record_passes(self) -> None:
        """Verify that a valid financial record passes schema validation."""
        valid_data = {
            "Date": VALID_PAST_DATE_STR,
            "ticker": "NVDA",
            "Open": 135.50,
            "High": 138.20,
            "Low": 134.10,
            "Close": 137.80,
            "Volume": 45000000,
        }
        record = MarketRecordSchema(**valid_data)
        assert record.ticker == "NVDA"
        assert record.Close == 137.80
        assert record.Volume == 45000000

    def test_negative_price_raises_validation_error(self) -> None:
        """Verify that negative stock prices trigger validation errors."""
        invalid_data = {
            "Date": VALID_PAST_DATE_STR,
            "ticker": "NVDA",
            "Open": -10.0,  # Invalid: must be > 0
            "High": 138.20,
            "Low": 134.10,
            "Close": 137.80,
            "Volume": 45000000,
        }
        with pytest.raises(ValidationError):
            MarketRecordSchema(**invalid_data)

    def test_zero_price_raises_validation_error(self) -> None:
        """Verify that a zero stock price triggers a validation error (prices must be > 0)."""
        invalid_data = {
            "Date": VALID_PAST_DATE_STR,
            "ticker": "NVDA",
            "Open": 135.0,
            "High": 138.0,
            "Low": 134.0,
            "Close": 0.0,  # Invalid: must be > 0
            "Volume": 1000,
        }
        with pytest.raises(ValidationError):
            MarketRecordSchema(**invalid_data)

    def test_negative_volume_raises_validation_error(self) -> None:
        """Verify that negative trading volume triggers a validation error."""
        invalid_data = {
            "Date": VALID_PAST_DATE_STR,
            "ticker": "TSM",
            "Open": 170.0,
            "High": 175.0,
            "Low": 168.0,
            "Close": 172.0,
            "Volume": -100,  # Invalid: must be >= 0
        }
        with pytest.raises(ValidationError):
            MarketRecordSchema(**invalid_data)

    def test_missing_required_field_raises_validation_error(self) -> None:
        """Verify that omitting a required field triggers a validation error."""
        incomplete_data = {
            "Date": VALID_PAST_DATE_STR,
            "ticker": "MSFT",
            "Open": 425.0,
            # Missing High, Low, Close, Volume
        }
        with pytest.raises(ValidationError):
            MarketRecordSchema(**incomplete_data)

    def test_future_date_raises_validation_error(self) -> None:
        """Verify that a record with a future date triggers a validation error."""
        invalid_data = {
            "Date": FUTURE_DATE_STR,
            "ticker": "NVDA",
            "Open": 150.0,
            "High": 155.0,
            "Low": 149.0,
            "Close": 152.0,
            "Volume": 5000000,
        }
        with pytest.raises(ValidationError) as exc_info:
            MarketRecordSchema(**invalid_data)
        assert "cannot be in the future" in str(exc_info.value)

    def test_high_less_than_low_raises_validation_error(self) -> None:
        """Verify that High price lower than Low price triggers a validation error."""
        invalid_data = {
            "Date": VALID_PAST_DATE_STR,
            "ticker": "NVDA",
            "Open": 135.0,
            "High": 130.0,  # Invalid: High < Low
            "Low": 140.0,
            "Close": 135.0,
            "Volume": 10000,
        }
        with pytest.raises(ValidationError) as exc_info:
            MarketRecordSchema(**invalid_data)
        assert "cannot be less than Low" in str(exc_info.value)

    def test_empty_ticker_raises_validation_error(self) -> None:
        """Verify that an empty string ticker raises a validation error."""
        invalid_data = {
            "Date": VALID_PAST_DATE_STR,
            "ticker": "",  # Invalid: empty string
            "Open": 100.0,
            "High": 105.0,
            "Low": 98.0,
            "Close": 102.0,
            "Volume": 10000,
        }
        with pytest.raises(ValidationError):
            MarketRecordSchema(**invalid_data)


# =====================================================================
# 2. Tests for Mathematical Precision of Technical Indicators
# =====================================================================
class TestMathematicalPrecision:
    """Test suite for verifying exact mathematical calculations in Pandas transformations."""

    def test_sma_7_mathematical_precision(self, sample_raw_dataframe: pd.DataFrame) -> None:
        """Verify that SMA-7 is calculated with exact mathematical precision."""
        transformer = MarketDataTransformer(s3_client=MagicMock())
        result_df = transformer.calculate_technical_indicators(sample_raw_dataframe)

        # sample_raw_dataframe Close: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        # At index 6 (7th observation):
        # SMA-7 = (10 + 20 + 30 + 40 + 50 + 60 + 70) / 7 = 280 / 7 = 40.0
        expected_sma_7_index_6 = 40.0
        assert result_df.loc[6, "SMA_7"] == pytest.approx(expected_sma_7_index_6, rel=1e-5)

        # At index 7 (8th observation):
        # SMA-7 = (20 + 30 + 40 + 50 + 60 + 70 + 80) / 7 = 350 / 7 = 50.0
        expected_sma_7_index_7 = 50.0
        assert result_df.loc[7, "SMA_7"] == pytest.approx(expected_sma_7_index_7, rel=1e-5)

        # At index 9 (10th observation):
        # SMA-7 = (40 + 50 + 60 + 70 + 80 + 90 + 100) / 7 = 490 / 7 = 70.0
        expected_sma_7_index_9 = 70.0
        assert result_df.loc[9, "SMA_7"] == pytest.approx(expected_sma_7_index_9, rel=1e-5)

    def test_sma_30_mathematical_precision(self) -> None:
        """Verify that SMA-30 calculates accurate 30-day moving average."""
        transformer = MarketDataTransformer(s3_client=MagicMock())

        # Construct a 35-day dataset where Close price = sequence from 1.0 to 35.0
        start_date = TODAY_UTC - timedelta(days=40)
        dates = pd.date_range(start=start_date, periods=35, freq="D")
        close_prices = [float(i + 1) for i in range(35)]
        df = pd.DataFrame({"Date": dates, "Close": close_prices})

        result_df = transformer.calculate_technical_indicators(df)

        # At index 29 (30th observation):
        # Sum of 1..30 = (30 * 31) / 2 = 465
        # Expected SMA-30 = 465 / 30 = 15.5
        expected_sma_30_at_29 = 15.5
        assert result_df.loc[29, "SMA_30"] == pytest.approx(expected_sma_30_at_29, rel=1e-5)

        # At index 34 (35th observation):
        # Window covers values from 6 to 35: Sum = (35*36)/2 - (5*6)/2 = 630 - 15 = 615
        # Expected SMA-30 = 615 / 30 = 20.5
        expected_sma_30_at_34 = 20.5
        assert result_df.loc[34, "SMA_30"] == pytest.approx(expected_sma_30_at_34, rel=1e-5)

    def test_daily_return_mathematical_precision(self) -> None:
        """Verify daily percentage return calculation formula ((Close_t - Close_{t-1}) / Close_{t-1})."""
        transformer = MarketDataTransformer(s3_client=MagicMock())

        start_date = TODAY_UTC - timedelta(days=10)
        dates = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(4)]

        # Day 0: 100.0 -> Daily Return: 0.0 (baseline)
        # Day 1: 110.0 -> Return: +10% (+0.10)
        # Day 2: 99.0  -> Return: (99 - 110) / 110 = -11 / 110 = -0.10 (-10%)
        # Day 3: 118.8 -> Return: (118.8 - 99) / 99 = +19.8 / 99 = +0.20 (+20%)
        df = pd.DataFrame({
            "Date": dates,
            "Close": [100.0, 110.0, 99.0, 118.8],
        })

        result_df = transformer.calculate_technical_indicators(df)

        assert result_df.loc[0, "daily_return"] == pytest.approx(0.0, abs=1e-6)
        assert result_df.loc[1, "daily_return"] == pytest.approx(0.10, rel=1e-5)
        assert result_df.loc[2, "daily_return"] == pytest.approx(-0.10, rel=1e-5)
        assert result_df.loc[3, "daily_return"] == pytest.approx(0.20, rel=1e-5)

    def test_volatility_7_mathematical_precision(self) -> None:
        """Verify 7-day rolling volatility calculation, null handling, and non-negativity."""
        transformer = MarketDataTransformer(s3_client=MagicMock())

        start_date = TODAY_UTC - timedelta(days=20)
        dates = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(10)]
        df = pd.DataFrame({
            "Date": dates,
            "Close": [100.0, 102.0, 101.0, 104.0, 103.0, 106.0, 105.0, 108.0, 107.0, 110.0],
        })

        result_df = transformer.calculate_technical_indicators(df)

        assert "volatility_7" in result_df.columns
        assert result_df["volatility_7"].isna().sum() == 0
        assert (result_df["volatility_7"] >= 0.0).all()
        assert result_df.loc[0, "volatility_7"] == pytest.approx(0.0, abs=1e-6)

        expected_std_index_6 = result_df.loc[0:6, "daily_return"].std()
        assert result_df.loc[6, "volatility_7"] == pytest.approx(expected_std_index_6, rel=1e-5)

    def test_clean_data_handles_nulls_via_forward_fill(self) -> None:
        """Verify that null values in financial indicators are properly imputed with forward-fill."""
        transformer = MarketDataTransformer(s3_client=MagicMock())

        start_date = TODAY_UTC - timedelta(days=10)
        dates = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(4)]

        df_with_nulls = pd.DataFrame({
            "Date": dates,
            "Open": [100.0, np.nan, 102.0, np.nan],
            "High": [105.0, 106.0, np.nan, 108.0],
            "Low": [95.0, np.nan, 97.0, 98.0],
            "Close": [102.0, np.nan, np.nan, 105.0],
            "Volume": [1000, np.nan, 2000, np.nan],
        })

        cleaned = transformer.clean_data(df_with_nulls)

        # Verify no NaNs remain
        assert not cleaned.isnull().values.any()
        # Row 1 Close was NaN, should be forward-filled from Row 0 (102.0)
        assert cleaned.loc[1, "Close"] == 102.0
        # Row 2 Close was NaN, should continue forward-fill (102.0)
        assert cleaned.loc[2, "Close"] == 102.0
        # Row 3 Close was 105.0
        assert cleaned.loc[3, "Close"] == 105.0


# =====================================================================
# 3. Tests for S3 I/O with Moto Simulation
# =====================================================================
class TestMarketDataTransformerS3Integration:
    """Test suite using moto to simulate AWS S3 read, transform, and write operations."""

    def test_read_bronze_parquet_success(
        self, mock_s3: Any, sample_raw_dataframe: pd.DataFrame
    ) -> None:
        """Test downloading and reading a real Parquet file from mocked S3 Bronze."""
        s3_key = "bronze/market_data/ticker=NVDA/year=2026/month=10/NVDA_test.parquet"

        # Upload a test parquet file to the mock S3 bucket
        buffer = io.BytesIO()
        sample_raw_dataframe.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        mock_s3.put_object(Bucket=TEST_BUCKET_NAME, Key=s3_key, Body=buffer.getvalue())

        transformer = MarketDataTransformer(s3_client=mock_s3)
        loaded_df = transformer.read_bronze_parquet(s3_key)

        assert len(loaded_df) == len(sample_raw_dataframe)
        assert list(loaded_df["Close"]) == list(sample_raw_dataframe["Close"])

    def test_read_bronze_parquet_file_not_found(self, mock_s3: Any) -> None:
        """Verify that attempting to read a non-existent S3 key raises FileNotFoundError."""
        transformer = MarketDataTransformer(s3_client=mock_s3)
        with pytest.raises(FileNotFoundError, match="not found"):
            transformer.read_bronze_parquet("bronze/market_data/ticker=NVDA/non_existent.parquet")

    def test_write_silver_parquet_success(
        self, mock_s3: Any, sample_raw_dataframe: pd.DataFrame
    ) -> None:
        """Test serializing and uploading transformed data to S3 Silver layer."""
        transformer = MarketDataTransformer(s3_client=mock_s3)
        enriched_df = transformer.calculate_technical_indicators(sample_raw_dataframe)

        s3_uri = transformer.write_silver_parquet(enriched_df, "NVDA")

        # Verify URI pattern
        expected_prefix = f"s3://{TEST_BUCKET_NAME}/silver/market_data/ticker=NVDA/"
        assert s3_uri.startswith(expected_prefix)
        assert s3_uri.endswith(".parquet")

        # Verify object actually exists in S3 and has proper content
        key = s3_uri.replace(f"s3://{TEST_BUCKET_NAME}/", "")
        response = mock_s3.get_object(Bucket=TEST_BUCKET_NAME, Key=key)
        assert response["ContentType"] == "application/vnd.apache.parquet"
        assert response["Metadata"]["ticker"] == "NVDA"
        assert response["Metadata"]["pipeline_stage"] == "silver_curated"

        # Verify Parquet payload integrity
        downloaded_df = pd.read_parquet(io.BytesIO(response["Body"].read()))
        assert "SMA_7" in downloaded_df.columns
        assert "SMA_30" in downloaded_df.columns
        assert "daily_return" in downloaded_df.columns

    def test_process_ticker_end_to_end(
        self, mock_s3: Any, sample_raw_dataframe: pd.DataFrame
    ) -> None:
        """Test complete end-to-end workflow: Bronze Parquet -> Transform -> Silver Parquet."""
        bronze_key = "bronze/market_data/ticker=TSM/year=2026/month=10/TSM_test.parquet"

        # Prepare bronze artifact for TSM
        tsm_df = sample_raw_dataframe.copy()
        tsm_df["ticker"] = "TSM"
        buffer = io.BytesIO()
        tsm_df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        mock_s3.put_object(Bucket=TEST_BUCKET_NAME, Key=bronze_key, Body=buffer.getvalue())

        transformer = MarketDataTransformer(s3_client=mock_s3)
        summary = transformer.process_ticker("TSM")

        assert summary["status"] == "SUCCESS"
        assert summary["ticker"] == "TSM"
        assert summary["records_processed"] == 10
        assert summary["bronze_source_key"] == bronze_key
        assert "silver/market_data/ticker=TSM/" in summary["silver_s3_uri"]

    def test_get_latest_bronze_key_raises_when_no_files(self, mock_s3: Any) -> None:
        """Verify that get_latest_bronze_key raises FileNotFoundError if no files exist."""
        transformer = MarketDataTransformer(s3_client=mock_s3)
        with pytest.raises(FileNotFoundError, match="No Bronze Parquet files found"):
            transformer.get_latest_bronze_key("MSFT")

    def test_validate_records_corrupt_data_raises_validation_error(self) -> None:
        """Verify that validate_records raises ValidationError when corrupted records exist."""
        transformer = MarketDataTransformer(s3_client=MagicMock())

        start_date = TODAY_UTC - timedelta(days=5)
        dates = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(2)]

        corrupt_df = pd.DataFrame({
            "Date": dates,
            "ticker": ["NVDA", "NVDA"],
            "Open": [100.0, -50.0],  # Row 1 has negative price
            "High": [105.0, 105.0],
            "Low": [95.0, 95.0],
            "Close": [102.0, 102.0],
            "Volume": [1000, 2000],
        })

        with pytest.raises(ValidationError):
            transformer.validate_records(corrupt_df)
