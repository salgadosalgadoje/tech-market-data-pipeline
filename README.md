# 🚀 Tech Market Data Pipeline (Medallion Architecture)

![CI Pipeline](https://github.com/salgadosalgadoje/tech-market-data-pipeline/actions/workflows/ci_pipeline.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.2-017CEE?logo=apacheairflow)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql)
![AWS S3](https://img.shields.io/badge/AWS-S3%20Data%20Lake-FF9900?logo=amazons3)

Un Data Pipeline financiero de nivel empresarial diseñado con **Arquitectura Medallón (Bronze ➔ Silver ➔ Gold)**, almacenamiento inmutable en **AWS S3 Data Lake**, validación de esquemas con **Pydantic V2**, enriquecimiento analítico con **Pandas**, persistencia relacional optimizada e idempotente en **PostgreSQL (SQLAlchemy)**, orquestación productiva con **Apache Airflow (LocalExecutor en Docker)** y suite de pruebas automatizadas con **Pytest / Moto / SQLite / DagBag**.

---

## 🏛️ Arquitectura del Sistema

```mermaid
flowchart LR
    A["Yahoo Finance API<br/>yfinance"] -->|"Fase 1: Extracción Raw"| B[("AWS S3 - Capa Bronce<br/>Parquet Particionado")]
    B -->|"Fase 2: Pydantic V2 + Pandas"| C[("AWS S3 - Capa Plata<br/>SMA-7, SMA-30, Retorno, Volatilidad")]
    C -->|"Fase 3: SQLAlchemy Upsert"| D[("PostgreSQL 16 - Capa Oro<br/>market_daily_metrics")]
    D -->|"Consumo Analítico"| E["Streamlit / FastAPI"]

    subgraph Orquestación y Calidad
        F["Apache Airflow 2.8.2<br/>LocalExecutor en Docker"] -.->|"0 18 * * 1-5"| A
        F -.-> B
        F -.-> C
        G["GitHub Actions CI/CD<br/>Pytest + Moto + DagBag"]
    end
```

---

## 📂 Estructura del Repositorio

```text
tech-market-data-pipeline/
│
├── .github/
│   └── workflows/
│       └── ci_pipeline.yml        # CI/CD: Pipeline automatizado en GitHub Actions (Pytest + Airflow)
│
├── dags/                          # Grafos Acíclicos Dirigidos (DAGs) de Apache Airflow
│   └── finance_etl_dag.py         # [Fase 4] DAG de orquestación diaria Medallón (Bronze -> Silver -> Gold)
│
├── logs/                          # Logs de ejecución de Apache Airflow (volumen montado)
│   └── .gitkeep
│
├── src/                           # Código fuente del pipeline ETL Medallion
│   ├── __init__.py
│   ├── extract_s3.py              # [Fase 1] Extracción y carga a AWS S3 (Bronze Layer)
│   ├── transform.py               # [Fase 2] Validación Pydantic y cálculo de indicadores (Silver Layer)
│   └── load_postgres.py           # [Fase 3] Modelo ORM, lectura S3 y Upsert en PostgreSQL (Gold Layer)
│
├── tests/                         # Suite de Pruebas Automatizadas (55 Items)
│   ├── __init__.py
│   ├── test_extract.py            # [Fase 1] Pruebas de extracción con mock_aws (Moto)
│   ├── test_transform.py          # [Fase 2] Validación de esquemas, indicadores y transformaciones
│   ├── test_load_postgres.py      # [Fase 3] Pruebas de integración con SQLite in-memory y Upsert
│   └── test_dag_integrity.py      # [Fase 4] Integridad estática AST, DagBag, aciclicidad y dependencias
│
├── .env.example                   # Plantilla de credenciales y variables de entorno
├── .gitignore                     # Protección de credenciales, logs y artefactos temporales
├── docker-compose.yml             # Infraestructura Docker: PostgreSQL 16 + Airflow (Webserver, Scheduler, Init)
├── pytest.ini                     # Configuración estandarizada para ejecución de Pytest
├── requirements.txt               # Dependencias fijadas y compatibles de producción y testing
└── README.md                      # Documentación técnica, arquitectónica y operativa
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
- **Precios Estrictamente Positivos**: `Open, High, Low, Close > 0` (`gt=0`).
- **Coherencia de Rango de Precios**: Valida que `High >= Low` a nivel de registro.
- **Volumen No Negativo**: `Volume >= 0` (`ge=0`).
- **Integridad Temporal**: Rechazo de registros con **fechas futuras** (`Date <= today_utc`).
- **Símbolo Ticker Válido**: `ticker` no puede ser vacío (`min_length=1`).

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

- **Volatilidad Móvil 7 Sesiones (Volatility 7)**: Desviación estándar móvil de los retornos diarios con `min_periods=1` y `.fillna(0.0)` para garantizar estabilidad numérica sin generar nulos en el día inicial:

$$\text{Volatilidad}_{7} = \sqrt{\frac{1}{N-1} \sum_{i=0}^{6} (\text{Retorno}_{t-i} - \overline{\text{Retorno}})^2}$$

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
| `volatility_7` | `FLOAT` | `NULLABLE` | Volatilidad móvil de 7 sesiones (desviación estándar) |
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

#### Ventajas Técnicas:
- **Idempotencia Estricta**: Ejecutar el pipeline 1 vez o 100 veces sobre el mismo rango de fechas produce el mismo estado en la base de datos sin duplicar métricas financieras.
- **Auto-corrección de Datos**: Si un dato histórico sufre un split o ajuste posterior en la Capa Plata, el Upsert actualiza automáticamente los registros existentes.
- **Dialecto Polimórfico**: El loader detecta dinámicamente si corre sobre `PostgreSQL` (en producción) o `SQLite` (en CI y tests locales), utilizando `dialects.postgresql.insert` o `dialects.sqlite.insert` respectivamente.

---

## ⏱️ Fase 4: Orquestación con Apache Airflow (Workflow Management)

La Fase 4 integra las 3 capas en un flujo de trabajo continuo, programado y tolerante a fallos orquestado por **Apache Airflow 2.8.2**, desplegado en contenedores Docker y configurado con **`LocalExecutor`** respaldado por PostgreSQL.

### 1. Definición del DAG (`dags/finance_etl_dag.py`)

El DAG `tech_market_etl_pipeline` implementa un flujo acíclico estricto con las siguientes especificaciones operativas:

- **Identificador**: `tech_market_etl_pipeline`
- **Cadencia Cron**: `0 18 * * 1-5` (Lunes a viernes a las 18:00 UTC / 6:00 PM EST, asegurando la captura de datos oficiales post-cierre de Wall Street).
- **Control de Ejecución**:
  - `catchup=False`: Previene ejecuciones históricas masivas accidentales al registrar el DAG por primera vez.
  - `max_active_runs=1`: Evita que ejecuciones consecutivas compitan por los mismos prefijos de S3 o bloqueen transacciones de base de datos.
- **Política de Reintentos (SLA & Resilience)**:
  - `retries=2`: Ante caídas transitorias de API o saturación de red en S3.
  - `retry_delay=timedelta(minutes=5)`: Espera progresiva antes de disparar el reintento.

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

### 2. Dependencias y Tareas Secuenciales

Las tareas utilizan el operador `PythonOperator` e invocan limpiamente los módulos principales del paquete `src`:

```
   ┌───────────────────────┐
   │  extract_bronze_task  │ ──► Ingestión Raw (yfinance ➔ S3 Bronze Parquet)
   └──────────┬────────────┘
              │
              ▼
   ┌───────────────────────┐
   │ transform_silver_task │ ──► Validación Pydantic V2 + Limpieza + Indicadores (S3 Silver)
   └──────────┬────────────┘
              │
              ▼
   ┌───────────────────────┐
   │    load_gold_task     │ ──► Carga e Idempotent Upsert en PostgreSQL (Gold Warehouse)
   └───────────────────────┘
```

- **Resolución Limpia de Rutas (`sys.path`)**:
  El DAG resuelve dinámicamente `PROJECT_ROOT` y `SRC_DIR` en tiempo de carga:
  ```python
  DAGS_DIR = Path(__file__).resolve().parent
  PROJECT_ROOT = DAGS_DIR.parent
  SRC_DIR = PROJECT_ROOT / "src"

  for path in (PROJECT_ROOT, SRC_DIR):
      if str(path) not in sys.path:
          sys.path.insert(0, str(path))
  ```
  Esto garantiza que tanto el Webserver como el Scheduler resuelvan `from src.extract_s3 import ...` sin requerir modificaciones manuales al entorno de ejecución.

- **Manejo Robusto de Excepciones**:
  Cada tarea valida el resultado devuelto (`summary["status"]`). Si se reporta `FAILURE` o `PARTIAL_FAILURE`, se eleva un `RuntimeError`, permitiendo que el motor de Airflow detecte el fallo y active su política de reintentos con métricas claras en los logs.

### 3. Infraestructura Docker Compose (`docker-compose.yml`)

La arquitectura de contenedores desacopla la persistencia de datos y el motor de orquestación utilizando anclas YAML (`x-airflow-common`):

1. **LocalExecutor Productivo**:
   Se evita el uso del `SequentialExecutor` (mono-hilo y basado en SQLite) en favor de `LocalExecutor` respaldado por PostgreSQL:
   ```yaml
   AIRFLOW__CORE__EXECUTOR: LocalExecutor
   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
   ```
2. **Aislamiento de Red y Host de Base de Datos**:
   - `env_file: [ .env ]` hereda automáticamente las credenciales de AWS, tickers y parámetros generales.
   - **Sobrescritura Crítica de Red**: Se fuerza `POSTGRES_HOST: postgres` en la configuración de Airflow para que resuelva el contenedor de la base de datos dentro de la red `tech_market_net`, evitando que intente conectarse al `localhost` del host.
3. **Volúmenes Persistentes**:
   - `./dags:/opt/airflow/dags`: Sincronización en vivo de los DAGs.
   - `./src:/opt/airflow/src`: Código fuente del pipeline ETL.
   - `./logs:/opt/airflow/logs`: Logs estructurados de tareas.
   - `postgres_data:/var/lib/postgresql/data`: Almacenamiento persistente de datos.
4. **Fijación de Compatibilidad Binaria**:
   Se asegura la compatibilidad entre paquetes en `_PIP_ADDITIONAL_REQUIREMENTS`:
   ```yaml
   _PIP_ADDITIONAL_REQUIREMENTS: "yfinance>=0.2.36 boto3>=1.34.0 pandas>=2.1.4 pydantic>=2.6.0 fastparquet>=2024.2.0 pyarrow<17.0.0 numpy<2.0.0"
   ```
   *(Las restricciones `pyarrow<17.0.0` y `numpy<2.0.0` previenen conflictos de extensiones C entre NumPy 2.x y PyArrow en entornos recreados).*
5. **Servicio `airflow-init`**:
   Ejecuta automáticamente las migraciones (`airflow db migrate`) y provisiona el usuario administrador (`admin` / `admin`) con validación de salud (`service_healthy`) de PostgreSQL.
6. **Airflow Webserver**:
   Expone el dashboard interactivo de monitoreo en el puerto `8080` con healthcheck automático sobre `/health`.

### 4. Pruebas de Integridad del DAG (`tests/test_dag_integrity.py`)

Se implementó una suite automatizada de pruebas con `pytest` estructurada en dos niveles:

1. **Validación Estática de AST (`TestDAGStaticStructure`)**:
   - Analiza el árbol sintáctico abstracto (AST) del DAG sin necesidad de tener Airflow instalado en el entorno anfitrión.
   - Valida compilación sin errores, presencia del identificador `tech_market_etl_pipeline`, la cadencia `0 18 * * 1-5`, las tres tareas y el operador de encadenamiento `>>`.
2. **Integridad Dinámica con `DagBag` (`TestAirflowDAGIntegrity`)**:
   - `test_dagbag_no_import_errors`: Confirma que `len(dagbag.import_errors) == 0`.
   - `test_dag_loaded_successfully`: Verifica que el DAG esté registrado.
   - `test_dag_no_cycles`: Ejecuta `check_cycle(dag)` para garantizar que no existan ciclos infinitos o bloqueos mutuos.
   - `test_dag_configuration_and_retries`: Valida schedule, `catchup=False`, `max_active_runs=1` y reintentos (`retries=2`, `retry_delay=timedelta(minutes=5)`).
   - `test_dag_has_exact_expected_tasks`: Comprueba la existencia exacta de las 3 tareas esperadas.
   - `test_dag_sequential_dependencies`: Valida rigurosamente la secuencia:
     - `extract_bronze_task` ➔ Upstream: `{}`, Downstream: `{"transform_silver_task"}`
     - `transform_silver_task` ➔ Upstream: `{"extract_bronze_task"}`, Downstream: `{"load_gold_task"}`
     - `load_gold_task` ➔ Upstream: `{"transform_silver_task"}`, Downstream: `{}`
3. **Manejo de Excepciones (`TestTaskExecutionLogic`)**:
   - Simula escenarios de falla en las capas y comprueba que las funciones ejecutoras lancen `RuntimeError` para activar los reintentos de Airflow.

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

## 🐳 Despliegue Completo con Docker Compose

El archivo [`docker-compose.yml`](docker-compose.yml) orquesta la base de datos PostgreSQL 16 y el ecosistema completo de Apache Airflow:

```bash
# 1. Iniciar toda la infraestructura en segundo plano
docker-compose up -d

# 2. Verificar el estado de los contenedores
docker-compose ps

# 3. Inspeccionar logs del scheduler o webserver si se requiere
docker-compose logs -f airflow-scheduler
```

### Acceso a la Interfaz Web de Airflow:
- **URL**: [http://localhost:8080](http://localhost:8080)
- **Usuario**: `admin`
- **Contraseña**: `admin`

Desde la interfaz de usuario de Airflow, activa el switch del DAG `tech_market_etl_pipeline` o ejecútalo manualmente haciendo clic en **Trigger DAG**.

Para ingresar a la consola interactiva de PostgreSQL:
```bash
docker exec -it tech_market_postgres psql -U postgres -d market_data
```

> 💡 **Nota sobre Base de Datos Existente**: Si ya contabas con un contenedor previo y deseas incorporar la columna `volatility_7` sin reiniciar el volumen persistente:
> ```bash
> docker exec -it tech_market_postgres psql -U postgres -d market_data -c "ALTER TABLE market_daily_metrics ADD COLUMN IF NOT EXISTS volatility_7 FLOAT;"
> ```

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

### 3. Ejecutar la Suite de Pruebas Automatizadas
La suite contiene **55 items de prueba** que garantizan la integridad de las 4 fases:

```bash
# Ejecutar todas las pruebas del repositorio (usando pytest.ini)
pytest -v

# Ejecutar las pruebas de integridad del DAG de Airflow
pytest tests/test_dag_integrity.py -v

# Ejecutar las pruebas de la Capa Oro (PostgreSQL Upsert)
pytest tests/test_load_postgres.py -v
```

Resultado de ejecución en entorno local:
```text
============================= test session starts =============================
configfile: pytest.ini
testpaths: tests
collected 55 items

tests/test_dag_integrity.py::TestDAGStaticStructure::test_dag_file_exists PASSED [  1%]
tests/test_dag_integrity.py::TestDAGStaticStructure::test_dag_syntax_compilation PASSED [  3%]
tests/test_dag_integrity.py::TestDAGStaticStructure::test_ast_contains_required_definitions PASSED [  5%]
tests/test_dag_integrity.py::TestAirflowDAGIntegrity::* SKIPPED (ejecutado en CI/Docker)
tests/test_extract.py .........                                          [ 36%]
tests/test_load_postgres.py ...............                              [ 65%]
tests/test_transform.py ....................                             [100%]

======================== 46 passed, 9 skipped in 7.68s ========================
```

---

## 🔄 Integración Continua (CI/CD con GitHub Actions)

El workflow [.github/workflows/ci_pipeline.yml](.github/workflows/ci_pipeline.yml) valida automáticamente cada push y pull request en ramas principales:

1. Levanta un runner en Ubuntu con Python 3.11.
2. Instala dependencias y fija Apache Airflow con su archivo de restricciones oficial:
   ```bash
   pip install "apache-airflow==2.8.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.2/constraints-3.11.txt"
   pip install -r requirements.txt
   ```
3. Ejecuta `airflow db migrate` para inicializar el metastore temporal en `/tmp/airflow`.
4. Ejecuta `pytest tests/ -v --tb=short` con credenciales mock de AWS (`mock_key`, `mock_secret`) y emulación S3 (`moto`).

---

## 📊 Verificación de Datos en PostgreSQL

Puedes conectarte a la base de datos y lanzar consultas analíticas para verificar la calidad de los datos cargados:

```sql
-- Consultar las métricas financieras más recientes con sus indicadores técnicos y volatilidad
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

## 🔮 Roadmap del Proyecto

- [x] **Fases 1–4**: Ingesta Bronze (AWS S3), Curación Silver (Pydantic V2 / Pandas), Warehouse Gold (PostgreSQL Upsert) y Orquestación Automatizada (Apache Airflow 2.8.2 + CI/CD en GitHub Actions).
- [ ] **Fase 5 (Próximo paso)**: Capa de consumo analítico mediante dashboard interactivo en Streamlit y endpoints REST en FastAPI.
- [ ] **Fase 6 (Producción Cloud)**: Despliegue continuo en AWS ECS / EC2 con base de datos administrada en AWS RDS y alertas de fallo vía Slack/Email.
