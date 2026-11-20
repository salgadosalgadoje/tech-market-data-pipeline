# 🚀 Tech Market Data Pipeline (Medallion Architecture)

![CI Pipeline](https://github.com/salgadosalgadoje/tech-market-data-pipeline/actions/workflows/ci_pipeline.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.2-017CEE?logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql)
![AWS S3](https://img.shields.io/badge/AWS-S3%20Data%20Lake-FF9900?logo=amazons3)

An enterprise-grade financial data pipeline engineered with **Medallion Architecture (Bronze ➔ Silver ➔ Gold)**, immutable cloud storage in an **AWS S3 Data Lake**, strict schema validation and data contracts with **Pydantic V2**, quantitative feature engineering with **Pandas**, optimized and idempotent relational persistence in **PostgreSQL (SQLAlchemy)**, production-grade scheduled orchestration with **Apache Airflow (LocalExecutor on Docker)**, an automated test suite with **Pytest / Moto / SQLite / DagBag**, and an analytical consumption layer powered by **FastAPI** and **Streamlit**.

---

## 🏛️ System Architecture

```mermaid
flowchart LR
    A["Yahoo Finance API<br/>yfinance"] -->|"Phase 1: Raw Ingestion"| B[("AWS S3 - Bronze Layer<br/>Partitioned Parquet")]
    B -->|"Phase 2: Pydantic V2 + Pandas"| C[("AWS S3 - Silver Layer<br/>SMA-7, SMA-30, Returns, Volatility")]
    C -->|"Phase 3: SQLAlchemy Upsert"| D[("PostgreSQL 16 - Gold Layer<br/>market_daily_metrics")]
    D -->|"Analytical Consumption"| E["Streamlit / FastAPI"]

    subgraph Orchestration and Quality
        F["Apache Airflow 2.8.2<br/>LocalExecutor on Docker"] -.->|"0 18 * * 1-5"| A
        F -.-> B
        F -.-> C
        G["GitHub Actions CI/CD<br/>Pytest + Moto + DagBag"]
    end
```

---

## 📂 Repository Structure

```text
tech-market-data-pipeline/
│
├── .github/
│   └── workflows/
│       └── ci_pipeline.yml        # CI/CD: Automated GitHub Actions pipeline (Pytest + Airflow)
│
├── dags/                          # Apache Airflow Directed Acyclic Graphs (DAGs)
│   └── finance_etl_dag.py         # [Phase 4] Daily Medallion orchestration DAG (Bronze -> Silver -> Gold)
│
├── logs/                          # Apache Airflow execution logs (mounted volume)
│   └── .gitkeep
│
├── src/                           # Medallion ETL pipeline source code
│   ├── __init__.py
│   ├── extract_s3.py              # [Phase 1] Market extraction & raw upload to AWS S3 (Bronze Layer)
│   ├── transform.py               # [Phase 2] Pydantic validation & indicator calculations (Silver Layer)
│   ├── load_postgres.py           # [Phase 3] ORM model, S3 reader & idempotent Upsert (Gold Layer)
│   ├── api/                       # [Phase 5] REST Analytical Consumption Layer (FastAPI)
│   │   ├── __init__.py
│   │   ├── schemas.py             # Strict Pydantic V2 response & request schemas
│   │   └── main.py                # REST endpoints and Dependency Injection
│   └── dashboard/                 # [Phase 5] Interactive Analytics Dashboard (Streamlit)
│       ├── __init__.py
│       └── app.py                 # Plotly visual analytics, executive KPIs & CSV export
│
├── tests/                         # Automated Test Suite (69 Test Items)
│   ├── __init__.py
│   ├── test_extract.py            # [Phase 1] Raw extraction tests with mock_aws (Moto)
│   ├── test_transform.py          # [Phase 2] Schema validation, indicators & transformation tests
│   ├── test_load_postgres.py      # [Phase 3] Integration tests with SQLite in-memory & Upsert logic
│   ├── test_dag_integrity.py      # [Phase 4] Static AST integrity, DagBag, acyclicity & dependencies
│   └── test_api.py                # [Phase 5] REST endpoint unit tests with TestClient & SQLite in-memory
│
├── .env.example                   # Environment variable template
├── .gitignore                     # Credentials, logs, and build artifact exclusions
├── docker-compose.yml             # Docker infrastructure: PostgreSQL 16 + Airflow + FastAPI + Streamlit
├── pytest.ini                     # Standardized Pytest test runner configuration
├── requirements.txt               # Pinned production and testing dependencies
└── README.md                      # Comprehensive technical documentation
```

---

## 🛠️ Phase 1: Raw Ingestion & Data Lake (Bronze Layer)

The Bronze layer implements an immutable market ingestion engine:
1. **Extraction**: Downloads daily historical market data for high-market-cap technology equities (`NVDA`, `TSM`, `MSFT`) via `yfinance`.
2. **Preliminary Validation**: Verifies that the incoming dataset is non-empty and contains essential trading fields (`Open`, `High`, `Low`, `Close`, `Volume`).
3. **Data Lake Storage**: Serializes datasets in memory to compressed **Apache Parquet** format and uploads partitioned files following Hive-style partitioning:
   ```text
   s3://<your-bucket>/bronze/market_data/ticker=NVDA/year=2026/month=10/NVDA_20261025_203000.parquet
   ```
4. **Metadata & Audit Trail**: Attaches custom HTTP metadata to each S3 object (`ticker`, `record_count`, `pipeline_stage=bronze_raw`, `ingestion_utc`).
5. **Isolated Testing**: Validated through unit tests using `moto[s3]` to simulate AWS S3 without cloud fees or live IAM credentials.

---

## 🥈 Phase 2: Transformation, Quality & Feature Engineering (Silver Layer)

The Silver layer converts raw payloads into cleaned, validated, and quantitatively enriched datasets:

### 1. Data Quality Validation with Pydantic V2 (`MarketRecordSchema`)
Enforces strict semantic integrity on every financial record before permitting entry into Silver:
- **Strictly Positive Prices**: `Open, High, Low, Close > 0` (`gt=0`).
- **Trading Range Consistency**: Validates that `High >= Low` at the record level.
- **Non-Negative Volume**: `Volume >= 0` (`ge=0`).
- **Temporal Consistency**: Rejection of records with **future dates** (`Date <= today_utc`).
- **Non-Empty Ticker Symbol**: `ticker` cannot be empty or whitespace (`min_length=1`).

### 2. Cleaning and Imputation with Pandas
- **Chronological Sorting**: Normalizes `Date` to calendar dates and sorts chronologically in ascending order.
- **Null Imputation**: Forward-fill (`ffill`) complemented by backward-fill (`bfill`) for edge initial rows, discarding residual corrupted records.

### 3. Quantitative Feature Engineering & Technical Indicators
- **SMA-7 (7-day Simple Moving Average)**: Short-term rolling moving average computed on the closing price (`Close`):

$$\text{SMA}_{7} = \frac{1}{7} \sum_{i=0}^{6} \text{Close}_{t-i}$$

- **SMA-30 (30-day Simple Moving Average)**: Intermediate-term rolling moving average computed on the closing price:

$$\text{SMA}_{30} = \frac{1}{30} \sum_{i=0}^{29} \text{Close}_{t-i}$$

- **Daily Return**: Daily percentage change in closing price. Baseline record initialized to 0.0 to prevent null artifacts:

$$\text{Return}_{t} = \frac{\text{Close}_{t} - \text{Close}_{t-1}}{\text{Close}_{t-1}}$$

- **7-Session Rolling Volatility**: Rolling standard deviation of daily returns computed with `min_periods=1` and `.fillna(0.0)` for numerical stability:

$$\text{Volatility}_{7} = \sqrt{\frac{1}{N-1} \sum_{i=0}^{6} (\text{Return}_{t-i} - \overline{\text{Return}})^2}$$

### 4. Curated Parquet Storage in S3 Silver
Enriched datasets are serialized as snappy-compressed Parquet files and partitioned by ticker:
```text
s3://<your-bucket>/silver/market_data/ticker=NVDA/year=2026/month=10/NVDA_silver_20261025_204500.parquet
```

---

## 🥇 Phase 3: Gold Layer / Data Warehouse (PostgreSQL Ingestion)

The **Gold Layer** materializes curated financial metrics into a relational data warehouse optimized for quantitative queries, real-time dashboards (Streamlit), and analytical REST endpoints (FastAPI).

### 1. Relational Data Model (`market_daily_metrics`)

The table models financial time series with atomic precision and normalized types:

| Column | SQL Type | Constraint / Property | Description |
| :--- | :--- | :--- | :--- |
| `ticker` | `VARCHAR(10)` | **PRIMARY KEY** (Composite) | Stock ticker symbol (e.g., `NVDA`, `TSM`, `MSFT`) |
| `date` | `DATE` | **PRIMARY KEY** (Composite) | Trading calendar date (`YYYY-MM-DD`) |
| `open` | `FLOAT` | `NOT NULL` | Opening session price in USD |
| `high` | `FLOAT` | `NOT NULL` | Highest traded price during the session in USD |
| `low` | `FLOAT` | `NOT NULL` | Lowest traded price during the session in USD |
| `close` | `FLOAT` | `NOT NULL` | Official closing price in USD |
| `volume` | `BIGINT` | `NOT NULL` | Total shares traded |
| `sma_7` | `FLOAT` | `NULLABLE` | 7-day Simple Moving Average |
| `sma_30` | `FLOAT` | `NULLABLE` | 30-day Simple Moving Average |
| `daily_return` | `FLOAT` | `NULLABLE` | Daily percentage return relative to previous close |
| `volatility_7` | `FLOAT` | `NULLABLE` | 7-day rolling historical volatility |
| `transformed_at_utc` | `TIMESTAMPTZ` | `NULLABLE` | Silver transformation processing timestamp |
| `loaded_at_utc` | `TIMESTAMPTZ` | `NOT NULL` | Gold ingestion timestamp |

### 2. Time-Series Indexing Strategy

B-Tree indexes optimize query throughput across common analytical access patterns:

```python
__table_args__ = (
    PrimaryKeyConstraint("ticker", "date", name="pk_market_daily_metrics"),
    Index("ix_market_daily_metrics_ticker_date", "ticker", "date"),  # Asset and temporal range queries
    Index("ix_market_daily_metrics_date", "date"),                  # Cross-sectional market aggregations
    Index("ix_market_daily_metrics_ticker", "ticker"),              # Ticker-specific filtering
)
```

- **`ix_market_daily_metrics_ticker_date`**: Accelerates visualization queries: `WHERE ticker = 'NVDA' AND date BETWEEN '2026-01-01' AND '2026-03-31'`.
- **`ix_market_daily_metrics_date`**: Powers cross-asset market return comparisons and cross-sectional volatility calculations.

### 3. Idempotent Upsert Pattern

In production pipelines, network retries or DAG re-executions can cause duplicate records if raw `INSERT` statements are used. The Gold Layer implements an **UPSERT** pattern:

```sql
INSERT INTO market_daily_metrics (ticker, date, open, high, low, close, volume, sma_7, sma_30, daily_return, volatility_7, transformed_at_utc, loaded_at_utc)
VALUES (...)
ON CONFLICT (ticker, date) 
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    sma_7 = EXCLUDED.sma_7,
    sma_30 = EXCLUDED.sma_30,
    daily_return = EXCLUDED.daily_return,
    volatility_7 = EXCLUDED.volatility_7,
    transformed_at_utc = EXCLUDED.transformed_at_utc,
    loaded_at_utc = EXCLUDED.loaded_at_utc;
```

#### Technical Guarantees:
- **Strict Idempotency**: Running the pipeline 1 or 100 times over the same date range leaves the database in an identical, deduplicated state.
- **Data Self-Correction**: If upstream indicators are revised in Silver, the Upsert updates existing records in place.
- **Polymorphic Dialects**: Dynamically uses `dialects.postgresql.insert` in production and `dialects.sqlite.insert` during CI/CD and unit testing.

---

## ⏱️ Phase 4: Workflow Orchestration with Apache Airflow

Phase 4 integrates the pipeline into an automated, fault-tolerant workflow managed by **Apache Airflow 2.8.2**, containerized via Docker and powered by **`LocalExecutor`** backed by PostgreSQL.

### 1. DAG Definition (`dags/finance_etl_dag.py`)

The DAG `tech_market_etl_pipeline` executes with the following operational specifications:
- **DAG ID**: `tech_market_etl_pipeline`
- **Schedule**: `0 18 * * 1-5` (Monday through Friday at 18:00 UTC / 6:00 PM EST, capturing official post-market closes).
- **Execution Policy**:
  - `catchup=False`: Prevents accidental historical execution floods.
  - `max_active_runs=1`: Avoids concurrent runs competing for the same S3 prefixes.
- **SLA & Retry Policy**:
  - `retries=2`: Recovers from transient network or rate-limiting glitches.
  - `retry_delay=timedelta(minutes=5)`: Progressive backoff between attempts.

```python
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
    extract_bronze_task >> transform_silver_task >> load_gold_task
```

### 2. Task Dependencies & Pipeline Execution

```
   ┌───────────────────────┐
   │  extract_bronze_task  │ ──► Raw Ingestion (yfinance ➔ S3 Bronze Parquet)
   └──────────┬────────────┘
              │
              ▼
   ┌───────────────────────┐
   │ transform_silver_task │ ──► Pydantic V2 Validation + Cleaning + Indicators (S3 Silver)
   └──────────┬────────────┘
              │
              ▼
   ┌───────────────────────┐
   │    load_gold_task     │ ──► Ingestion & Idempotent Upsert into PostgreSQL (Gold Warehouse)
   └───────────────────────┘
```

- **Dynamic Module Resolution (`sys.path`)**: Resolves `PROJECT_ROOT` and `SRC_DIR` at DAG load time, ensuring reliable imports in Docker.
- **Exception Propagation**: Validates return states (`summary["status"]`). Raises `RuntimeError` upon failure to trigger Airflow's native retry policies.

---

## 📈 Phase 5: Analytical Consumption Layer (FastAPI REST API & Streamlit Dashboard)

**Phase 5** establishes the analytical consumption layer connected directly to the **PostgreSQL Gold Layer**:
1. **High-Performance REST API (`src/api/main.py`)**: Built with **FastAPI**, featuring strict Pydantic V2 schemas, interactive Swagger/OpenAPI documentation, and dependency injection.
2. **Executive Analytics Dashboard (`src/dashboard/app.py`)**: Interactive **Streamlit** user interface with real-time KPI metrics, **Plotly** financial charts, multi-asset comparative analysis, and CSV data export.

```mermaid
flowchart TD
    subgraph Gold Layer
        DB[("PostgreSQL 16<br/>market_daily_metrics")]
    end

    subgraph Consumption Layer
        API["FastAPI REST API<br/>:8000<br/>• /health<br/>• /api/v1/tickers<br/>• /api/v1/metrics/{ticker}<br/>• /api/v1/metrics/{ticker}/latest<br/>• /api/v1/summary"]
        DASH["Streamlit Dashboard<br/>:8501<br/>• KPI Cards (Close, Return, Volatility)<br/>• OHLC Candlestick & SMA-7 / SMA-30<br/>• Volume & 7d Volatility Dynamics<br/>• Base 100 Relative Comparison"]
    end

    DB -->|"SQLAlchemy Session<br/>Depends(get_db)"| API
    DB -->|"SQLAlchemy Engine<br/>@st.cache_data"| DASH
    API -.->|"OpenAPI JSON / Docs"| CONSUMER["Downstream Systems / BI / Analysts"]
```

### 1. REST API Endpoints (FastAPI)

All endpoints utilize dependency injection (`Depends(get_db)`), reusing `MarketDailyMetrics` and `PostgresLoaderConfig`:

| Method | Endpoint | Query Parameters | HTTP Status | Description |
| :--- | :--- | :--- | :--- | :--- |
| `GET` | `/health` | None | `200`, `500` | Health check probe and live PostgreSQL connectivity verification (`SELECT 1`). |
| `GET` | `/api/v1/tickers` | None | `200`, `500` | Alphabetical list of all equity tickers currently populated in the Gold Layer. |
| `GET` | `/api/v1/metrics/{ticker}` | `start_date`, `end_date`, `limit` | `200`, `404`, `500` | Chronological time-series (`date ASC`), filterable by date range and record limit. |
| `GET` | `/api/v1/metrics/{ticker}/latest` | None | `200`, `404`, `500` | Most recent trading session record including all technical indicators. |
| `GET` | `/api/v1/summary` | None | `200`, `500` | Cross-sectional comparative summary (close, return, 7d average volatility, volume). |

#### Pydantic V2 Schemas (`src/api/schemas.py`)
- `MarketMetricResponse`: Strict type modeling with `model_config = ConfigDict(from_attributes=True)` for seamless SQLAlchemy serialization.
- `TickerListResponse`: Catalog of available symbols and total count.
- `SummaryResponse` / `TickerSummaryItem`: Cross-asset financial aggregation schema.
- `HealthResponse`: Service health status, database connectivity, and UTC timestamp.

---

### 2. Interactive Streamlit Dashboard (`src/dashboard/app.py`)

Executive-grade interface tailored for financial analysts and decision-makers:
- **Top KPI Cards**: Dynamic `st.metric` widgets tracking Latest Close, Daily Return %, 7d Rolling Volatility %, and Traded Volume.
- **Main Trend Chart**: Interactive toggle between Line Chart with `SMA-7` / `SMA-30` overlays and `OHLC Candlestick` chart.
- **Volume & Risk Profiling**: Daily traded volume bar chart (color-coded by positive/negative return) and dual-axis 7d rolling volatility vs. return.
- **Multi-Asset Performance Comparison (Base 100)**: Normalized time-series performance comparison starting from a common base date.
- **Market Summary Matrix & Export**: Full tabular summary across all tracked tickers with a dedicated `.csv` download button.
- **Fault-Tolerant UI**: Graceful exception handling for database connectivity issues with actionable troubleshooting guidance and reconnect options.

---

## ☁️ Configuration Guide: AWS S3 & PostgreSQL

### 1. Environment Variables (`.env`)
Copy the provided `.env.example` file and configure your credentials:

```bash
cp .env.example .env
```

Key environment parameters:
```env
# AWS S3 Data Lake
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=tech-market-bronze-jsalgado
S3_BRONZE_PREFIX=bronze/market_data
S3_SILVER_PREFIX=silver/market_data

# Pipeline Scope
TARGET_TICKERS=NVDA,TSM,MSFT
EXTRACTION_PERIOD=1mo
EXTRACTION_INTERVAL=1d

# PostgreSQL / Gold Layer
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_secure_password_here
POSTGRES_DB=market_data
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
DATABASE_URL=postgresql+psycopg2://postgres:your_secure_password_here@localhost:5432/market_data
```

> ⚠️ **IMPORTANT**: Never commit your `.env` file to source control. It is explicitly ignored via `.gitignore`.

---

## 🐳 Deployment with Docker Compose

The [`docker-compose.yml`](docker-compose.yml) orchestrates PostgreSQL 16, Apache Airflow, and the analytical consumption layer:

```bash
# 1. Start core data infrastructure (PostgreSQL + Airflow) in the background
docker-compose up -d

# 2. Check running container status
docker-compose ps

# 3. View Airflow scheduler or webserver logs
docker-compose logs -f airflow-scheduler

# 4. (Optional - Phase 5) Start the Analytical Consumption Layer (FastAPI + Streamlit):
docker-compose --profile consumption up -d
```

### Platform URLs & Default Ports:
- **Airflow Webserver**: [http://localhost:8080](http://localhost:8080) (Username: `admin`, Password: `admin`)
- **FastAPI REST API Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Streamlit Analytics Dashboard**: [http://localhost:8501](http://localhost:8501)

To access the interactive PostgreSQL shell:
```bash
docker exec -it tech_market_postgres psql -U postgres -d market_data
```

---

## 💻 Local Installation & Execution

### 1. Create and Activate Virtual Environment
```bash
# On Windows (PowerShell):
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# On Linux/macOS:
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the Analytical Consumption Layer Locally

#### A. Start the FastAPI REST API:
Starts the FastAPI server with hot-reloading:
```bash
uvicorn src.api.main:app --reload
```
- **Interactive Swagger / OpenAPI Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Redoc Documentation**: [http://localhost:8000/redoc](http://localhost:8000/redoc)
- **Health Check**: [http://localhost:8000/health](http://localhost:8000/health)

#### B. Start the Streamlit Analytics Dashboard:
Launches the interactive dashboard connected to the Gold Layer:
```bash
streamlit run src/dashboard/app.py
```
- **Local Dashboard URL**: [http://localhost:8501](http://localhost:8501)

---

### 4. Execute the Automated Test Suite
The test suite contains **69 comprehensive test items** verifying all 5 pipeline phases:

```bash
# Run the entire test suite (using pytest.ini)
pytest -v

# Run the Analytical REST API tests (Phase 5 - FastAPI + SQLite in-memory)
pytest tests/test_api.py -v

# Run Airflow DAG static integrity tests (Phase 4)
pytest tests/test_dag_integrity.py -v

# Run Gold Layer tests (Phase 3 - PostgreSQL Upsert)
pytest tests/test_load_postgres.py -v
```

Local test execution output:
```text
============================= test session starts =============================
configfile: pytest.ini
testpaths: tests
collected 69 items

tests/test_api.py ...............                                        [ 20%]
tests/test_dag_integrity.py::TestDAGStaticStructure::* PASSED           [ 24%]
tests/test_dag_integrity.py::TestAirflowDAGIntegrity::* SKIPPED (tested in CI/Docker)
tests/test_extract.py .........                                          [ 50%]
tests/test_load_postgres.py ...............                              [ 72%]
tests/test_transform.py ....................                             [100%]

======================== 60 passed, 9 skipped in 11.07s =======================
```

---

## 🔄 Continuous Integration (CI/CD with GitHub Actions)

The [.github/workflows/ci_pipeline.yml](.github/workflows/ci_pipeline.yml) workflow validates every push and pull request against primary branches:

1. Provisions an Ubuntu runner on Python 3.11.
2. Installs dependencies and pins Apache Airflow using its official constraints file:
   ```bash
   pip install "apache-airflow==2.8.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.2/constraints-3.11.txt"
   pip install -r requirements.txt
   ```
3. Runs `airflow db migrate` to initialize a temporary metadata store at `/tmp/airflow`.
4. Executes `pytest tests/ -v --tb=short` with mocked AWS credentials (`mock_key`, `mock_secret`), S3 emulation (`moto`), and an isolated in-memory SQLite database for FastAPI.

---

## 📊 Gold Layer Verification Queries

You can connect directly to PostgreSQL and run analytical SQL queries:

```sql
-- Query the most recent market metrics and technical indicators
SELECT 
    ticker,
    date,
    close,
    sma_7,
    sma_30,
    ROUND((daily_return * 100)::numeric, 2) AS return_pct,
    ROUND((volatility_7 * 100)::numeric, 2) AS vol_7_pct,
    volume,
    loaded_at_utc
FROM market_daily_metrics
ORDER BY ticker, date DESC
LIMIT 10;
```

---

## 🔮 Project Roadmap

- [x] **Phases 1–4**: Bronze Ingestion (AWS S3), Silver Curation (Pydantic V2 / Pandas), Gold Warehouse (PostgreSQL Upsert), and Automated Orchestration (Apache Airflow 2.8.2 + CI/CD in GitHub Actions).
- [x] **Phase 5**: Analytical Consumption Layer via interactive Streamlit dashboard and FastAPI REST endpoints.
- [ ] **Phase 6 (Cloud Production)**: Continuous deployment on AWS ECS / EC2 with AWS RDS managed database and failure alerting via Slack/Email.
