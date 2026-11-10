# 🚀 Tech Market Data Pipeline (Medallion Architecture)

Un Data Pipeline financiero de nivel empresarial diseñado con **Arquitectura Medallón (Bronze ➔ Silver ➔ Gold)**, almacenamiento inmutable en **AWS S3 Data Lake**, validación de esquemas con **Pydantic**, enriquecimiento analítico con **Pandas**, almacenamiento analítico en **PostgreSQL**, orquestación con **Apache Airflow** y pruebas automatizadas con **Pytest / Moto**.

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
                                           ▼ (Fase 3: Almacenamiento Estructurado)
                ┌────────────────────────────────────────────────────────┐
                │               CAPA ORO (GOLD / ANALYTICS)              │
                │  - PostgreSQL (vía SQLAlchemy / Alembic)               │
                │  - Consumo: Dashboards (Streamlit) / API (FastAPI)     │
                └────────────────────────────────────────────────────────┘

    [Orquestación: Apache Airflow (Docker)]  ◄---►  [CI/CD: GitHub Actions + Pytest]
```

El proyecto implementa una canalización de datos (Data Pipeline) basada en la arquitectura Medallón (Bronce, Plata, Oro) para el procesamiento de datos del mercado, orquestada mediante Apache Airflow y con despliegue automatizado vía CI/CD.

![Diagrama de Arquitectura del Pipeline de Datos](market_data_pipeline_architecture.jpg)

---

## 📂 Estructura del Repositorio

```text
tech-market-pipeline/
│
├── dags/                          # Grafos Acíclicos Dirigidos (DAGs) de Apache Airflow
│   └── finance_etl_dag.py
│
├── src/                           # Código fuente del pipeline ETL
│   ├── __init__.py
│   ├── extract_s3.py              # [Fase 1] Descarga con yfinance y carga en AWS S3 (Bronze)
│   ├── transform.py               # [Fase 2] Modelos Pydantic V2 y transformaciones con Pandas (Silver)
│   └── load_postgres.py           # [Fase 3] Modelos SQLAlchemy y carga a PostgreSQL (Gold)
│
├── tests/                         # Suite de Pruebas Unitarias e Integración
│   ├── __init__.py
│   ├── test_extract.py            # [Fase 1] Pruebas de extracción con pytest y emulación moto (S3)
│   └── test_transform.py          # [Fase 2] Validación de esquemas, indicadores y transformaciones
│
├── .env.example                   # Plantilla de credenciales y configuración
├── .gitignore                     # Protección de secretos y archivos temporales
├── docker-compose.yml             # Despliegue de PostgreSQL y Airflow en contenedores
├── requirements.txt               # Dependencias de producción y testing
└── README.md                      # Documentación completa del proyecto
```

---

## 🛠️ Fase 1: Extractor y Data Lake (Bronze Layer)

En esta fase se implementa el motor de ingestión inmutable:
1. **Extracción**: Descarga datos históricos diarios de activos tecnológicos de alta capitalización (`NVDA`, `TSM`, `MSFT`) mediante `yfinance`.
2. **Validación Preliminar**: Comprobación de que el DataFrame no esté vacío y contenga las columnas esenciales (`Open`, `High`, `Low`, `Close`, `Volume`).
3. **Almacenamiento en Data Lake**: Serialización en memoria a formato **Apache Parquet** comprimido y subida particionada estilo Hive:
   ```text
   s3://<tu-bucket>/bronze/market_data/ticker=NVDA/year=2026/month=10/NVDA_20261025_203000.parquet
   ```
4. **Metadatos y Auditoría**: Inclusión de metadatos HTTP en el objeto S3 (`ticker`, `record_count`, `pipeline_stage=bronze_raw`, `ingestion_utc`).
5. **Testing Aislado**: Pruebas con `moto[s3]` que emulan S3 sin requerir credenciales reales ni generar costos de nube.

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
  $$\text{SMA}_7 = \frac{1}{7} \sum_{i=0}^{6} \text{Close}_{t-i}$$
- **SMA-30 (Simple Moving Average 30 días)**: Media móvil de medio plazo calculada sobre el precio de cierre:
  $$\text{SMA}_{30} = \frac{1}{30} \sum_{i=0}^{29} \text{Close}_{t-i}$$
- **Retorno Diario (Daily Return)**: Variación porcentual diaria del precio de cierre:
  $$\text{Retorno}_t = \frac{\text{Close}_t - \text{Close}_{t-1}}{\text{Close}_{t-1}}$$
  *(El día base inicial se normaliza a `0.0` para evitar valores nulos).*

### 4. Almacenamiento Curado en S3 Silver
Los datos enriquecidos se serializan como Parquet y se almacenan bajo la estructura particionada:
```text
s3://<tu-bucket>/silver/market_data/ticker=NVDA/year=2026/month=10/NVDA_silver_20261025_204500.parquet
```

---

## ☁️ Guía de Configuración: AWS S3 y Credenciales

Para ejecutar las cargas hacia la nube real de AWS:

### 1. Crear Cuenta en AWS
Ingresa a [aws.amazon.com](https://aws.amazon.com/) y regístrate en la **Capa Gratuita (Free Tier)**.

### 2. Crear el Bucket S3
1. En la consola de AWS, busca el servicio **S3**.
2. Haz clic en **Create bucket**.
3. Asigna un nombre globalmente único, por ejemplo: `tech-market-data-lake-<tu-nombre>`.
4. Selecciona tu región preferida (ej. `us-east-1`).
5. Deja activada la opción **Block all public access** (seguridad por defecto).

### 3. Crear Usuario IAM y Access Keys
1. En AWS IAM, ve a **Users** ➔ **Create user** (`pipeline-s3-writer`).
2. Asigna permisos directos (`AmazonS3FullAccess` o política restringida a tu bucket).
3. En la pestaña **Security credentials**, crea una **Access Key** para CLI / Aplicación externa.
4. Guarda de forma segura el `Access Key ID` y `Secret Access Key`.

### 4. Configurar el archivo `.env`
Copia la plantilla y configura tus variables de entorno:
```bash
cp .env.example .env
```

Parámetros en `.env`:
```ini
AWS_ACCESS_KEY_ID=tu_access_key_aqui
AWS_SECRET_ACCESS_KEY=tu_secret_key_aqui
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=tech-market-data-lake-<tu-nombre>
S3_BRONZE_PREFIX=bronze/market_data
S3_SILVER_PREFIX=silver/market_data
TARGET_TICKERS=NVDA,TSM,MSFT
EXTRACTION_PERIOD=1mo
EXTRACTION_INTERVAL=1d
STORAGE_FORMAT=parquet
LOG_LEVEL=INFO
```

> ⚠️ **IMPORTANTE**: Nunca compartas ni subas tu archivo `.env` al control de versiones. Está incluido en `.gitignore`.

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

### 3. Ejecutar la Suite de Pruebas Automatizadas (Pytest + Moto)
Las pruebas unitarias y de integración emulan AWS S3 en memoria con `moto`, ejecutándose sin necesidad de credenciales de AWS:

```bash
# Ejecutar todas las pruebas (Fase 1 + Fase 2)
pytest tests/ -v

# Ejecutar específicamente las pruebas de la Capa Plata (Silver)
pytest tests/test_transform.py -v
```

Resultado esperado (27 pruebas exitosas):
```text
============================= test session starts =============================
collected 27 items

tests/test_extract.py .........                                          [ 33%]
tests/test_transform.py ..................                               [100%]

============================= 27 passed in 5.42s ==============================
```

### 4. Ejecución del Pipeline

#### Paso A: Extracción e Ingestión a Bronce (Raw Layer)
```bash
python -m src.extract_s3
```

#### Paso B: Transformación y Carga a Plata (Silver Layer)
```bash
python -m src.transform
```

Salida esperada:
```text
[2026-10-25 20:45:10] [INFO] [market_pipeline.transform]: ==================================================
[2026-10-25 20:45:10] [INFO] [market_pipeline.transform]: 🚀 STARTING TECH MARKET TRANSFORMATION PIPELINE (SILVER)
[2026-10-25 20:45:10] [INFO] [market_pipeline.transform]: Target Tickers: ['NVDA', 'TSM', 'MSFT']
[2026-10-25 20:45:10] [INFO] [market_pipeline.transform]: S3 Bucket: tech-market-bronze-jsalgado
[2026-10-25 20:45:10] [INFO] [market_pipeline.transform]: ==================================================
[2026-10-25 20:45:11] [INFO] [market_pipeline.transform]: Reading Bronze Parquet from s3://.../NVDA_20261025.parquet
[2026-10-25 20:45:11] [INFO] [market_pipeline.transform]: Validating 22 records against MarketRecordSchema...
[2026-10-25 20:45:11] [INFO] [market_pipeline.transform]: Calculated SMA_7, SMA_30, and daily_return for 22 records.
[2026-10-25 20:45:12] [INFO] [market_pipeline.transform]: Successfully uploaded Silver Parquet to s3://...

Silver Pipeline Execution Report:
---------------------------------
✅ NVDA: 22 records -> s3://tech-market-bronze-jsalgado/silver/market_data/ticker=NVDA/...
✅ TSM: 22 records -> s3://tech-market-bronze-jsalgado/silver/market_data/ticker=TSM/...
✅ MSFT: 22 records -> s3://tech-market-bronze-jsalgado/silver/market_data/ticker=MSFT/...
```

---

## 🔮 Roadmap del Proyecto
- [x] **Fase 1: Capa Bronce (Raw S3 Data Lake & Moto Testing)**
  - Ingestión desde Yahoo Finance y almacenamiento inmutable en Apache Parquet particionado.
  - Pruebas automatizadas con emulación moto de AWS S3.
- [x] **Fase 2: Capa Plata (Silver / Curated Layer)**
  - Validación de esquemas con Pydantic V2 (precios positivos, fechas válidas, coherencia High/Low).
  - Limpieza de datos (imputación forward-fill / backward-fill).
  - Cálculo de indicadores técnicos: SMA-7, SMA-30 y Retorno Diario.
  - Almacenamiento curado en Parquet particionado en S3 Silver.
  - Suite de 18 pruebas unitarias con precisión matemática y emulación S3.
- [ ] **Fase 3: Capa Oro (Gold / Analytics Layer)**: Carga en PostgreSQL mediante SQLAlchemy / Alembic para consultas analíticas de baja latencia.
- [ ] **Fase 4: Orquestación**: Apache Airflow DAG (`finance_etl_dag.py`) en Docker Compose con alertas automáticas.
- [ ] **Fase 5: CI/CD**: Automatización con GitHub Actions para linter y ejecución de pruebas en cada pull request.
