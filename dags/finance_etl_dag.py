"""Apache Airflow DAG for Tech Market Data Pipeline.

Orchestrates the Medallion Architecture (Bronze -> Silver -> Gold)
for high-tech equities (NVDA, TSM, MSFT) daily at market close.

Schedule:
    Monday through Friday at 18:00 UTC (Post-market close): 0 18 * * 1-5

Architecture Flow:
    1. extract_bronze_task (Bronze Layer):
       Extracts daily historical price and volume data via Yahoo Finance (yfinance)
       and persists immutable raw Parquet datasets into the AWS S3 Bronze Data Lake.
    2. transform_silver_task (Silver Layer):
       Reads raw Parquet from S3 Bronze, performs data quality & schema validation
       with Pydantic, cleans missing values, computes technical indicators (SMA-7,
       SMA-30, daily returns) with Pandas, and stores curated Parquet in S3 Silver.
    3. load_gold_task (Gold Layer):
       Consumes curated Silver Parquet from S3, prepares records against SQLAlchemy ORM
       models, and executes dialect-aware idempotent UPSERT into PostgreSQL table
       `market_daily_metrics` for downstream analytics and BI.
"""

from datetime import datetime, timedelta
import logging
from pathlib import Path
import sys
from typing import Any, Dict

# ==============================================================================
# Path Management: Ensure Airflow Workers and Schedulers recognize 'src' package
# ==============================================================================
DAGS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DAGS_DIR.parent
SRC_DIR = PROJECT_ROOT / "src"

for path in (PROJECT_ROOT, SRC_DIR):
    str_path = str(path)
    if str_path not in sys.path:
        sys.path.insert(0, str_path)

from airflow import DAG
from airflow.operators.python import PythonOperator

# Configure structured task logger
logger = logging.getLogger("airflow.task.tech_market_etl")


def execute_extract_bronze() -> Dict[str, Any]:
    """Execute Bronze Layer: Extract raw financial market data and write to S3 Data Lake.

    Invokes `TechMarketExtractionPipeline` from `src.extract_s3`.
    Validates execution status and raises RuntimeError if any target ticker fails,
    enabling Airflow's automatic retry policy.

    Returns:
        Dict[str, Any]: Summary dictionary with ingestion metrics and S3 URIs.

    Raises:
        RuntimeError: If the bronze extraction reports FAILURE or PARTIAL_FAILURE.
    """
    logger.info("==================================================")
    logger.info("🚀 [TASK START] extract_bronze_task (Medallion Bronze Layer)")
    logger.info("==================================================")

    from src.extract_s3 import TechMarketExtractionPipeline

    try:
        pipeline = TechMarketExtractionPipeline()
        report = pipeline.run()

        status = report.get("status", "UNKNOWN")
        failed = report.get("failed_tickers", [])

        if status != "SUCCESS" or len(failed) > 0:
            error_msg = (
                f"Bronze layer extraction completed with issues (Status: {status}). "
                f"Failed tickers: {failed}"
            )
            logger.error("❌ %s", error_msg)
            raise RuntimeError(error_msg)

        logger.info(
            "✅ [TASK SUCCESS] Bronze extraction completed successfully. "
            "Processed tickers: %s",
            list(report.get("tickers_processed", {}).keys()),
        )
        return report

    except Exception as exc:
        logger.error("💥 Unhandled exception in extract_bronze_task: %s", exc, exc_info=True)
        raise


def execute_transform_silver() -> Dict[str, Any]:
    """Execute Silver Layer: Cleanse, validate with Pydantic, enrich with technical indicators.

    Invokes `MarketDataTransformer` from `src.transform`.
    Reads Bronze Parquet from AWS S3, cleans data, calculates SMA-7, SMA-30, and returns,
    and writes curated Parquet to AWS S3 Silver.

    Returns:
        Dict[str, Any]: Summary dictionary with transformation metrics.

    Raises:
        RuntimeError: If silver transformation reports FAILURE or PARTIAL_FAILURE.
    """
    logger.info("==================================================")
    logger.info("🚀 [TASK START] transform_silver_task (Medallion Silver Layer)")
    logger.info("==================================================")

    from src.transform import MarketDataTransformer

    try:
        transformer = MarketDataTransformer()
        report = transformer.run()

        status = report.get("status", "UNKNOWN")
        failed = report.get("failed_tickers", [])

        if status != "SUCCESS" or len(failed) > 0:
            error_msg = (
                f"Silver layer transformation completed with issues (Status: {status}). "
                f"Failed tickers: {failed}"
            )
            logger.error("❌ %s", error_msg)
            raise RuntimeError(error_msg)

        logger.info(
            "✅ [TASK SUCCESS] Silver transformation completed successfully. "
            "Curated tickers: %s",
            list(report.get("tickers_processed", {}).keys()),
        )
        return report

    except Exception as exc:
        logger.error("💥 Unhandled exception in transform_silver_task: %s", exc, exc_info=True)
        raise


def execute_load_gold() -> Dict[str, Any]:
    """Execute Gold Layer: Ingest curated Silver Parquet into PostgreSQL Data Warehouse.

    Invokes `PostgresDataLoader` from `src.load_postgres`.
    Reads latest curated Silver Parquet from S3 and performs dialect-aware
    idempotent UPSERT (`ON CONFLICT (ticker, date) DO UPDATE`) into `market_daily_metrics`.

    Returns:
        Dict[str, Any]: Summary dictionary with upserted record counts.

    Raises:
        RuntimeError: If gold loading reports FAILURE or PARTIAL_FAILURE.
    """
    logger.info("==================================================")
    logger.info("🚀 [TASK START] load_gold_task (Medallion Gold Layer)")
    logger.info("==================================================")

    from src.load_postgres import PostgresDataLoader

    try:
        loader = PostgresDataLoader()
        report = loader.load_all_target_tickers()

        status = report.get("status", "UNKNOWN")
        failed = report.get("failed_tickers", [])

        if status != "SUCCESS" or len(failed) > 0:
            error_msg = (
                f"Gold layer ingestion completed with issues (Status: {status}). "
                f"Failed tickers: {failed}"
            )
            logger.error("❌ %s", error_msg)
            raise RuntimeError(error_msg)

        logger.info(
            "✅ [TASK SUCCESS] Gold warehouse ingestion completed successfully. "
            "Upserted tickers: %s",
            list(report.get("tickers_processed", {}).keys()),
        )
        return report

    except Exception as exc:
        logger.error("💥 Unhandled exception in load_gold_task: %s", exc, exc_info=True)
        raise


# ==============================================================================
# DAG Definition & Configuration
# ==============================================================================
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="tech_market_etl_pipeline",
    default_args=default_args,
    description="Daily Medallion ETL pipeline for tech stocks (NVDA, TSM, MSFT) post market close",
    schedule="0 18 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["finance", "medallion", "s3", "postgres", "etl"],
) as dag:

    extract_bronze_task = PythonOperator(
        task_id="extract_bronze_task",
        python_callable=execute_extract_bronze,
        doc_md="""
        ### Bronze Layer Ingestion Task
        - **Source**: Yahoo Finance API (`yfinance`)
        - **Target**: AWS S3 Bronze Data Lake (`bronze/market_data/ticker=.../`)
        - **Format**: Parquet (Immutable raw layer)
        """,
    )

    transform_silver_task = PythonOperator(
        task_id="transform_silver_task",
        python_callable=execute_transform_silver,
        doc_md="""
        ### Silver Layer Transformation Task
        - **Source**: AWS S3 Bronze Data Lake
        - **Validation**: Pydantic schema validation (`MarketRecordSchema`)
        - **Enrichment**: SMA-7, SMA-30, daily returns, null-handling
        - **Target**: AWS S3 Silver Curated Lake (`silver/market_data/ticker=.../`)
        """,
    )

    load_gold_task = PythonOperator(
        task_id="load_gold_task",
        python_callable=execute_load_gold,
        doc_md="""
        ### Gold Layer Warehouse Loading Task
        - **Source**: AWS S3 Silver Curated Lake
        - **Target**: PostgreSQL table `market_daily_metrics`
        - **Operation**: Dialect-aware idempotent UPSERT (`ON CONFLICT (ticker, date) DO UPDATE`)
        """,
    )

    # Medallion Architecture Sequential Pipeline Dependency
    extract_bronze_task >> transform_silver_task >> load_gold_task
