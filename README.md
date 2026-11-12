# 🚀 Tech Market Data Pipeline (Medallion Architecture)

Un Data Pipeline financiero de nivel empresarial diseñado con **Arquitectura Medallón (Bronze ➔ Silver ➔ Gold)**, almacenamiento inmutable en **AWS S3 Data Lake**, validación de esquemas con **Pydantic V2**, enriquecimiento analítico con **Pandas**, persistencia relacional optimizada e idempotente en **PostgreSQL (SQLAlchemy)**, orquestación lista para **Apache Airflow** y pruebas automatizadas con **Pytest / Moto / SQLite**.

---

## 🏛️ Arquitectura General del Sistema

```
                    ┌──────────────────────────────────────────────┐
                    │               FUENTES EXTERNAS               │
                    │         Yahoo Finance API (yfinance)         │
                    └──────────────────────┬───────────────────────┘
                                           │
                                           ▼ (Fase 1: Extracción Raw)
                ┌────────────────────────────────────────────────────────┐
                │              CAPA BRONCE (RAW DATA LAKE)               │
                │  AWS S3 Bucket: tech-market-bronze-jsalgado            │
                │  Path: bronze/market_data/ticker=NVDA/year=2026/...    │
                │  Format: Apache Parquet (Inmutable, Comprimido)        │
                └───────────────────────────┬────────────────────────────┘
                                           │
                                           ▼ (Fase 2: Limpieza & Validación)
                ┌────────────────────────────────────────────────────────┐
                │             CAPA PLATA (SILVER / CURATED)              │
                │  - Validación estricta con Pydantic V2                 │
                │  - Imputación de nulos (ffill/bfill) con Pandas        │
                │  - Indicadores técnicos (SMA-7, SMA-30, Retorno Diario)│
                │  Path: silver/market_data/ticker=NVDA/year=2026/...    │
                │  Format: Apache Parquet Curado                         │
                └───────────────────────────┬────────────────────────────┘
                                           │
                                           ▼ (Fase 3: Warehouse & Analytics)
                ┌────────────────────────────────────────────────────────┐
                │               CAPA ORO (GOLD / ANALYTICS)              │
                │  - Modelo ORM: MarketDailyMetrics (SQLAlchemy 2.x)     │
                │  - Carga Idempotente: INSERT ... ON CONFLICT DO UPDATE │
                │  - Clave Primaria Compuesta: (ticker, date)            │
                │  - Índices B-Tree Optimizados para Series Temporales   │
                │  - Base de Datos: PostgreSQL 16 (Docker)               │
                │  - Consumo: Dashboards (Streamlit) / API (FastAPI)     │
                └────────────────────────────────────────────────────────┘

    [Orquestación: Apache Airflow (Docker)]  ◄---►  [CI/CD: GitHub Actions + Pytest]
```

![Diagrama de Arquitectura del Pipeline de Datos](market_data_pipeline_architecture.jpg)

---

## 📂 Estructura del Repositorio

```text
tech-market-pipeline/
│
├── dags/                          # Grafos Acíclicos Dirigidos (DAGs) de Apache Airflow
│   └── finance_etl_dag.py         # Definición de DAG orquestador diario
│
├── src/                           # Código fuente del pipeline ETL Medallion
│   ├── __init__.py
│   ├── extract_s3.py              # [Fase 1] Extracción y carga a AWS S3 (Bronze Layer)
│   ├── transform.py               # [Fase 2] Validación Pydantic y cálculo de indicadores (Silver Layer)
│   └── load_postgres.py           # [Fase 3] Modelo ORM, lectura S3 y Upsert en PostgreSQL (Gold Layer)
│
├── tests/                         # Suite de Pruebas Unitarias e Integración (42 Tests)
│   ├── __init__.py
│   ├── test_extract.py            # [Fase 1] Pruebas de extracción con mock_aws (Moto)
│   ├── test_transform.py          # [Fase 2] Validación de esquemas, indicadores y transformaciones
│   └── test_load_postgres.py      # [Fase 3] Pruebas de integración con SQLite in-memory y Upsert
│
├── .env.example                   # Plantilla de credenciales y variables de entorno
├── .gitignore                     # Protección de credenciales y artefactos temporales
├── docker-compose.yml             # Contenedores para PostgreSQL 16 y Apache Airflow
├── requirements.txt               # Dependencias de producción y suite de pruebas
└── README.md                      # Documentación arquitectónica y operativa
```

---

## 🛠️ Fase 1: Extractor y Data Lake (Bronze Layer)

En esta fase se implementa el motor de ingestión inmutable de mercado:
1. **Extracción**: Descarga datos históricos diarios de activos tecnológicos de alta capitalización (`NVDA`, `TSM`, `MSFT`) mediante `yfinance`.
2. **Validación Preliminar**: Comprobación de que el DataFrame no esté vacío y contenga las columnas esenciales (`Open`, `High`, `Low`, `Close`, `Volume`).
3. **Almacenamiento en Data Lake**: Serialización en memoria a formato **Apache Parquet** comprimido y subida particionada estilo Hive:
   ```text
   s3://<tu-bucket>/bronze/market_data/ticker=NVDA/year=2026/month=10/NVDA_20261025_203000.parquet
   ```
4. **Metadatos y Auditoría**: Inclusión de metadatos HTTP en el objeto S3 (`ticker`, `record_count`, `pipeline_stage=bronze_raw`, `ingestion_utc`).
5. **Testing Aislado**: 9 pruebas con `moto[s3]` que emulan S3 sin requerir credenciales reales ni generar costos de nube.

---

## 🥈 Fase 2: Transformación, Calidad y Enriquecimiento (Silver Layer)

La Capa Plata convierte datos crudos en datasets limpios, validados y analíticamente enriquecidos:

### 1. Validación de Calidad de Datos con Pydantic V2 (`MarketRecordSchema`)
Garantiza la coherencia semántica de cada registro antes de permitir su paso a Silver:
- **Precios Estrictamente Positivos**: `Open`, `High`, `Low` y `Close` deben ser mayores a 0 (`gt=0`).
- **Coherencia de Rango de Precios**: Valida que `High >= Low` a nivel de registro.
- **Volumen No Negativo**: `Volume >= 0`.
- **Integridad Temporal**: Rechazo de registros con **fechas futuras** (`Date <= today_utc`).
- **Símbolo Ticker Válido**: `ticker` no puede ser vacío.

### 2. Limpieza e Imputación con Pandas
- **Orden Cronológico**: Conversión del campo `Date` a tipo datetime y orden ascendente.
- **Tratamiento de Nulos**: Imputación de valores faltantes mediante forward-fill (`ffill`) complementado con backward-fill (`bfill`) para casos iniciales, descartando filas residuales no recuperables.

### 3. Ingeniería de Características e Indicadores Técnicos
- **SMA-7 (Simple Moving Average 7 días)**: Media móvil calculada sobre el precio de cierre (`Close`) para capturar la tendencia de corto plazo:

$$\text{SMA}_{7} = \frac{1}{7} \sum_{i=0}^{6} \text{Close}_{t-i}$$

- **SMA-30 (Simple Moving Average 30 días)**: Media móvil de medio plazo calculada sobre el precio de cierre:

$$\text{SMA}_{30} = \frac{1}{30} \sum_{i=0}^{29} \text{Close}_{t-i}$$

- **Retorno Diario (Daily Return)**: Variación porcentual diaria del precio de cierre. El día base inicial se imputa a 0.0 para evitar valores nulos al inicio de la serie temporal:

$$\text{Retorno}_{t} = \frac{\text{Close}_{t} - \text{Close}_{t-1}}{\text{Close}_{t-1}}$$

### 4. Almacenamiento Curado en S3 Silver
Los datos enriquecidos se serializan como Parquet y se almacenan bajo la estructura particionada:
```text
s3://<tu-bucket>/silver/market_data/ticker=NVDA/year=2026/month=10/NVDA_silver_20261025_204500.parquet
```

---

## 🥇 Fase 3: Capa Oro / Analytics Layer (PostgreSQL Warehouse)

La **Capa Oro** materializa los datos curados en una base de datos relacional de alto rendimiento, optimizada para analistas cuantitativos, dashboards en tiempo real (Streamlit) y APIs analíticas (FastAPI).

### 1. Modelo de Datos Relacional (`market_daily_metrics`)

El esquema de la tabla modela series temporales financieras con precisión atómica y tipos de datos normalizados:

| Columna | Tipo SQL | Restricción / Propiedad | Descripción |
| :--- | :--- | :--- | :--- |
| `ticker` | `VARCHAR(10)` | **PRIMARY KEY** (Compuesta) | Símbolo bursátil del activo (ej. `NVDA`, `TSM`, `MSFT`) |
| `date` | `DATE` | **PRIMARY KEY** (Compuesta) | Fecha de la jornada bursátil (`YYYY-MM-DD`) |
| `open` | `FLOAT` | `NOT NULL` | Precio de apertura en USD |
| `high` | `FLOAT` | `NOT NULL` | Precio máximo operado en el día en USD |
| `low` | `FLOAT` | `NOT NULL` | Precio mínimo operado en el día en USD |
| `close` | `FLOAT` | `NOT NULL` | Precio de cierre oficial en USD |
| `volume` | `BIGINT` | `NOT NULL` | Volumen total de acciones negociadas |
| `sma_7` | `FLOAT` | `NULLABLE` | Media Móvil Simple de 7 sesiones |
| `sma_30` | `FLOAT` | `NULLABLE` | Media Móvil Simple de 30 sesiones |
| `daily_return` | `FLOAT` | `NULLABLE` | Retorno porcentual respecto al cierre previo |
| `transformed_at_utc` | `TIMESTAMPTZ` | `NULLABLE` | Marca temporal de procesamiento en Capa Plata |
| `loaded_at_utc` | `TIMESTAMPTZ` | `NOT NULL` | Marca temporal de ingestión en Capa Oro |

### 2. Estrategia de Indexación para Series Temporales

Se establecieron índices B-Tree específicos para maximizar el throughput en patrones de consulta analíticos:

```python
__table_args__ = (
    PrimaryKeyConstraint("ticker", "date", name="pk_market_daily_metrics"),
    Index("ix_market_daily_metrics_ticker_date", "ticker", "date"),  # Queries por activo y ventana temporal
    Index("ix_market_daily_metrics_date", "date"),                  # Agregaciones transversales de mercado
    Index("ix_market_daily_metrics_ticker", "ticker"),              # Filtros agregados por símbolo
)
```

- **`ix_market_daily_metrics_ticker_date`**: Optimiza consultas típicas de visualización: `WHERE ticker = 'NVDA' AND date BETWEEN '2026-01-01' AND '2026-03-31'`.
- **`ix_market_daily_metrics_date`**: Permite calcular correlaciones, retornos agregados y comparaciones inter-activo para una misma fecha bursátil.

### 3. Carga Idempotente (Upsert Pattern)

En ingeniería de datos de producción, las fallas transitorias de red o las re-ejecuciones de tareas en Airflow pueden provocar duplicación de registros si se usa un simple `INSERT`. 

La Capa Oro implementa el patrón **UPSERT** mediante la cláusula nativa:
```sql
INSERT INTO market_daily_metrics (ticker, date, open, high, low, close, volume, sma_7, sma_30, daily_return, transformed_at_utc, loaded_at_utc)
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
    transformed_at_utc = EXCLUDED.transformed_at_utc,
    loaded_at_utc = EXCLUDED.loaded_at_utc;
```

#### Ventajas Técnicas:
- **Idempotencia Estricta**: Ejecutar el pipeline 1 vez o 100 veces sobre el mismo rango de fechas produce el mismo estado en la base de datos sin duplicar métricas financieras.
- **Auto-corrección de Datos**: Si un dato histórico sufre un split o ajuste posterior en la Capa Plata, el Upsert actualiza automáticamente los registros existentes.
- **Dialecto Polimórfico**: El loader detecta dinámicamente si corre sobre `PostgreSQL` (en producción) o `SQLite` (en CI y tests locales), utilizando `dialects.postgresql.insert` o `dialects.sqlite.insert` respectivamente.

### 4. Lectura Inteligente desde S3 Silver
- Conexión vía `boto3` para explorar el prefijo `silver/market_data/ticker={ticker}/`.
- Detección automática del Parquet más reciente para cada activo mediante ordenación lexicográfica y timestamps ISO.
- Streaming a memoria vía `io.BytesIO` y parseo con `pyarrow`.

---

## ☁️ Guía de Configuración: AWS S3 y PostgreSQL

### 1. Variables de Entorno (`.env`)
Copia la plantilla `.env.example` y configura tus credenciales:

```bash
cp .env.example .env
```

Parámetros clave:
```env
# AWS S3 Data Lake
AWS_ACCESS_KEY_ID=tu_access_key_aqui
AWS_SECRET_ACCESS_KEY=tu_secret_key_aqui
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=tech-market-bronze-jsalgado
S3_BRONZE_PREFIX=bronze/market_data
S3_SILVER_PREFIX=silver/market_data

# Pipeline Scope
TARGET_TICKERS=NVDA,TSM,MSFT
EXTRACTION_PERIOD=1mo
EXTRACTION_INTERVAL=1d

# PostgreSQL / Capa Oro
POSTGRES_USER=postgres
POSTGRES_PASSWORD=tu_password_seguro
POSTGRES_DB=market_data
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
DATABASE_URL=postgresql+psycopg2://postgres:tu_password_seguro@localhost:5432/market_data
```

> ⚠️ **IMPORTANTE**: Nunca compartas ni subas tu archivo `.env` a repositorios públicos. El archivo se encuentra excluido mediante `.gitignore`.

---

## 🐳 Despliegue de la Base de Datos Local con Docker

El repositorio incluye un [`docker-compose.yml`](docker-compose.yml) listo para producción local que despliega PostgreSQL 16 con volumen persistente y healthcheck:

```bash
# 1. Iniciar el servicio PostgreSQL en segundo plano
docker-compose up -d postgres

# 2. Verificar el estado del contenedor
docker-compose ps postgres

# 3. (Opcional) Inspeccionar los logs del motor relacional
docker-compose logs -f postgres
```

Para ingresar a la consola interactiva de PostgreSQL:
```bash
docker exec -it tech_market_postgres psql -U postgres -d market_data
```

---

## 💻 Instalación y Ejecución Local

### 1. Crear y Activar Entorno Virtual
```bash
# En Windows (PowerShell):
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# En Linux/macOS:
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Instalar Dependencias
```bash
pip install -r requirements.txt
```

### 3. Ejecutar la Suite de Pruebas Automatizadas (Pytest + Moto + SQLite)
El pipeline cuenta con **42 pruebas automatizadas** que cubren las 3 capas sin necesidad de infraestructura de nube real ni bases de datos activas en CI:

```bash
# Ejecutar la suite completa (42 pruebas)
pytest tests/ -v

# Ejecutar específicamente las pruebas de la Capa Oro (15 pruebas de integración)
pytest tests/test_load_postgres.py -v
```

#### Cobertura de Pruebas de la Capa Oro:
- ✅ `test_table_creation_and_composite_primary_key`: Creación de DDL y validación de PK compuesta `(ticker, date)`.
- ✅ `test_table_indexes_configuration`: Verificación de índices B-Tree optimizados.
- ✅ `test_strict_upsert_insert_then_update_single_record`: **Prueba de fuego del Upsert** (inserción de un registro y actualización posterior del mismo `ticker/date`, verificando conteo exacto de 1 fila y actualización precisa de métricas).
- ✅ `test_batch_upsert_idempotence`: Ingestión doble de un lote completo comprobando duplicación cero.
- ✅ `test_batch_upsert_partial_overlap_and_new_records`: Ingestión de lotes con intersecciones y registros nuevos.
- ✅ `test_prepare_records_converts_types_and_handles_nan`: Normalización snake_case, tipos fecha y sustitución de `NaN` a `None` (SQL NULL).
- ✅ `test_s3_read_silver_parquet_and_upsert_flow`: Integración end-to-end con bucket mock S3 (`moto`) y base de datos SQLite.
- ✅ `test_get_latest_silver_key_selection`: Selección precisa del artefacto Parquet más reciente.

Resultado esperado:
```text
============================= test session starts =============================
collected 42 items

tests/test_extract.py .........                                          [ 21%]
tests/test_load_postgres.py ...............                              [ 57%]
tests/test_transform.py ..................                               [100%]

============================= 42 passed in 11.36s =============================
```

---

### 4. Ejecución del Pipeline Medallón de Extremo a Extremo

#### Paso A: Extracción e Ingestión a Bronce (Raw Layer)
```bash
python -m src.extract_s3
```

#### Paso B: Transformación y Carga a Plata (Silver Layer)
```bash
python -m src.transform
```

#### Paso C: Ingestión Idempotente a Oro (Gold Layer / PostgreSQL)
```bash
python -m src.load_postgres
```

Salida esperada de la Capa Oro:
```text
[2026-11-12 13:00:00] [INFO] [market_pipeline.load_postgres]: ==================================================
[2026-11-12 13:00:00] [INFO] [market_pipeline.load_postgres]: 🚀 STARTING TECH MARKET GOLD INGESTION PIPELINE
[2026-11-12 13:00:00] [INFO] [market_pipeline.load_postgres]: Target Tickers: ['NVDA', 'TSM', 'MSFT']
[2026-11-12 13:00:00] [INFO] [market_pipeline.load_postgres]: Target Table: market_daily_metrics
[2026-11-12 13:00:00] [INFO] [market_pipeline.load_postgres]: ==================================================
[2026-11-12 13:00:01] [INFO] [market_pipeline.load_postgres]: Discovered latest Silver Parquet for ticker 'NVDA': silver/market_data/ticker=NVDA/...parquet
[2026-11-12 13:00:01] [INFO] [market_pipeline.load_postgres]: Successfully read 22 records from s3://...
[2026-11-12 13:00:01] [INFO] [market_pipeline.load_postgres]: Executing idempotent upsert of 22 records (Dialect: postgresql)...
[2026-11-12 13:00:01] [INFO] [market_pipeline.load_postgres]: Successfully upserted 22 records into 'market_daily_metrics'.

Gold Layer Ingestion Report:
----------------------------
✅ NVDA: 22 records upserted from silver/market_data/ticker=NVDA/year=2026/month=11/NVDA_silver_...parquet
✅ TSM: 22 records upserted from silver/market_data/ticker=TSM/year=2026/month=11/TSM_silver_...parquet
✅ MSFT: 22 records upserted from silver/market_data/ticker=MSFT/year=2026/month=11/MSFT_silver_...parquet
```

---

## 📊 Verificación de Datos en PostgreSQL

Puedes conectarte a la base de datos y lanzar consultas analíticas para verificar la calidad de los datos cargados:

```sql
-- Consultar las métricas financieras más recientes con sus indicadores técnicos
SELECT 
    ticker,
    date,
    close,
    sma_7,
    sma_30,
    ROUND((daily_return * 100)::numeric, 2) AS return_pct,
    volume,
    loaded_at_utc
FROM market_daily_metrics
ORDER BY ticker, date DESC
LIMIT 10;
```

---

## 🔮 Roadmap del Proyecto
- [x] **Fase 1: Capa Bronce (Raw S3 Data Lake & Moto Testing)**
  - Ingestión desde Yahoo Finance y almacenamiento inmutable en Apache Parquet particionado.
  - Pruebas automatizadas con emulación `moto` de AWS S3.
- [x] **Fase 2: Capa Plata (Silver / Curated Layer)**
  - Validación de esquemas con Pydantic V2 (precios positivos, fechas válidas, coherencia High/Low).
  - Limpieza de datos (imputación forward-fill / backward-fill).
  - Cálculo de indicadores técnicos: SMA-7, SMA-30 y Retorno Diario.
  - Almacenamiento curado en Parquet particionado en S3 Silver.
- [x] **Fase 3: Capa Oro (Gold / Analytics Layer - PostgreSQL)**
  - Modelo ORM `MarketDailyMetrics` con SQLAlchemy 2.x e indexación B-Tree de series temporales.
  - Lógica de Upsert idempotente (`INSERT ... ON CONFLICT (ticker, date) DO UPDATE`).
  - Suite de 15 pruebas de integración con base de datos SQLite en memoria y simulación S3.
- [ ] **Fase 4: Orquestación con Apache Airflow**: DAG diario (`finance_etl_dag.py`) en Docker Compose con dependencias secuenciales Bronce ➔ Plata ➔ Oro y alertas.
- [ ] **Fase 5: CI/CD y Visualización**: Automatización con GitHub Actions y Dashboard interactivo en Streamlit / API en FastAPI.
