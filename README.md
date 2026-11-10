# 🚀 Tech Market Data Pipeline (Medallion Architecture)

Un Data Pipeline financiero de nivel empresarial diseñado con **Arquitectura Medallón (Bronze ➔ Silver ➔ Gold)**, almacenamiento inmutable en **AWS S3 Data Lake**, validación de esquemas con **Pydantic**, almacenamiento analítico en **PostgreSQL**, orquestación con **Apache Airflow** y pruebas automatizadas con **Pytest / Moto**.

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
               │  - Validación estricta con Pydantic                    │
               │  - Imputación de nulos & normalización con Pandas      │
               │  - Cálculo de métricas (SMA-7, SMA-30, Volatilidad)    │
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
│   ├── transform.py               # [Fase 2] Modelos Pydantic y transformaciones con Pandas (Silver)
│   └── load_postgres.py           # [Fase 3] Modelos SQLAlchemy y carga a PostgreSQL (Gold)
│
├── tests/                         # Suite de Pruebas Unitarias e Integración
│   ├── __init__.py
│   ├── test_extract.py            # [Fase 1] Pruebas con pytest y emulación AWS con moto
│   └── test_transform.py          # [Fase 2] Validación de reglas de transformación
│
├── .env.example                   # Plantilla de credenciales y configuración
├── .gitignore                     # Protección de secretos y archivos temporales
├── docker-compose.yml             # Despliegue de PostgreSQL y Airflow en contenedores
├── requirements.txt               # Dependencias de producción y testing
└── README.md                      # Documentación completa del proyecto
```

---

## 🛠️ Fase 1: Extractor y Data Lake (Bronze Layer)

En esta primera fase se implementa el motor de extracción inmutable:
1. **Extracción**: Descarga los datos históricos del último mes para acciones tecnológicas clave (`NVDA`, `TSM`, `MSFT`) mediante `yfinance`.
2. **Validación Preliminar**: Comprobación estricta de que el dataset no esté vacío y contenga las columnas financieras esenciales (`Open`, `High`, `Low`, `Close`, `Volume`).
3. **Almacenamiento en Data Lake**: Serialización en memoria a formato **Apache Parquet** (o CSV) y subida particionada estilo Hive:
   ```text
   s3://<tu-bucket>/bronze/market_data/ticker=NVDA/year=2026/month=10/NVDA_20261025_203000.parquet
   ```
4. **Metadatos y Trazabilidad**: Cada objeto subido a S3 incluye metadatos HTTP (ticker, conteo de registros, etapa del pipeline, timestamp UTC).
5. **Testing 100% aislado**: Pruebas con `moto[s3]` que simulan AWS S3 en memoria sin requerir conexión real ni generar costos.

---

## ☁️ Guía Paso a Paso: Configurar AWS S3 y Credenciales

Para ejecutar la carga hacia la nube real de AWS:

### 1. Crear Cuenta en AWS
1. Ingresa a [aws.amazon.com](https://aws.amazon.com/) y regístrate para la **Capa Gratuita (Free Tier)** (incluye 5 GB de almacenamiento S3 por 12 meses).

### 2. Crear el Bucket S3
1. En la consola de AWS, busca el servicio **S3**.
2. Haz clic en **Create bucket (Crear bucket)**.
3. Elige un nombre único a nivel mundial, por ejemplo: `tech-market-data-lake-<tu-nombre>`.
4. Selecciona tu región preferida (ej: `us-east-1` Norte de Virginia).
5. Deja activada la opción **Block all public access** (los datos deben ser privados).
6. Haz clic en **Create bucket**.

### 3. Crear Usuario IAM y Access Keys (Seguridad de Mínimo Privilegio)
1. En el buscador de AWS, ve a **IAM** (Identity and Access Management).
2. En el menú lateral, haz clic en **Users (Usuarios)** ➔ **Create user**.
3. Nombre de usuario: `pipeline-s3-writer`.
4. En permisos, selecciona **Attach policies directly** y busca `AmazonS3FullAccess` (o crea una política con permisos solo a tu bucket específico).
5. Finaliza la creación del usuario.
6. Entra al usuario creado, ve a la pestaña **Security credentials (Credenciales de seguridad)**.
7. En la sección **Access keys**, haz clic en **Create access key** ➔ Selecciona **Application running outside AWS**.
8. **Copia** el `Access Key ID` y el `Secret Access Key`.

### 4. Configurar el archivo `.env`
Copia la plantilla y coloca tus credenciales:
```bash
cp .env.example .env
```
Edita `.env`:
```ini
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=tech-market-data-lake-<tu-nombre>
TARGET_TICKERS=NVDA,TSM,MSFT
STORAGE_FORMAT=parquet
```

> ⚠️ **IMPORTANTE**: Nunca subas tu archivo `.env` a GitHub. Está incluido en `.gitignore` por defecto.

---

## 💻 Instalación y Ejecución Local

### 1. Crear y Activar Entorno Virtual
```bash
# En Windows (PowerShell):
python -m venv venv
.\venv\Scripts\Activate.ps1

# En Linux/macOS:
python3 -m venv venv
source venv/bin/activate
```

### 2. Instalar Dependencias
```bash
pip install -r requirements.txt
```

### 3. Ejecutar la Suite de Pruebas (Pytest + Moto)
Las pruebas corren 100% en local emulando S3 con `moto`:
```bash
pytest tests/ -v
```

### 4. Ejecutar el Pipeline de Extracción (Bronze)
```bash
python -m src.extract_s3
```

Salida esperada:
```text
[2026-10-25 20:30:15] [INFO] [market_pipeline.extract_s3]: Starting extraction for ticker: NVDA (Period: 1mo, Interval: 1d)
[2026-10-25 20:30:16] [INFO] [market_pipeline.extract_s3]: Successfully fetched 22 records for NVDA
[2026-10-25 20:30:17] [INFO] [market_pipeline.extract_s3]: Uploading PARQUET artifact to s3://tech-market-bronze-jsalgado/bronze/market_data/ticker=NVDA/year=2026/month=10/NVDA_20261025_203017.parquet
[2026-10-25 20:30:18] [INFO] [market_pipeline.extract_s3]: Successfully uploaded artifact to s3://tech-market-bronze-jsalgado/...

Pipeline Execution Report:
-------------------------
✅ NVDA: 22 records -> s3://tech-market-bronze-jsalgado/bronze/market_data/ticker=NVDA/...
✅ TSM: 22 records -> s3://tech-market-bronze-jsalgado/bronze/market_data/ticker=TSM/...
✅ MSFT: 22 records -> s3://tech-market-bronze-jsalgado/bronze/market_data/ticker=MSFT/...
```

---

## 🔮 Roadmap: Próximas Fases
- [x] **Fase 1: Capa Bronce (Raw S3 Data Lake & Moto Testing)**
- [ ] **Fase 2: Capa Plata (Silver Layer)**: Validación con esquemas Pydantic V2, cálculo de medias móviles y detección de outliers con Pandas.
- [ ] **Fase 3: Capa Oro (Gold Layer)**: Inserción en PostgreSQL mediante SQLAlchemy y Alembic para consultas analíticas de baja latencia.
- [ ] **Fase 4: Orquestación**: Apache Airflow DAG (`finance_etl_dag.py`) en Docker Compose con alertas automáticas.
- [ ] **Fase 5: CI/CD**: GitHub Actions workflow para validación de linter (`ruff`/`black`) y ejecución automática de `pytest` en cada push.
