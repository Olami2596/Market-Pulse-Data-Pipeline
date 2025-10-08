# Market Pulse Data Pipeline

Market Pulse is a modern data engineering project that automates the extraction, transformation, and loading (ETL) of global stock market data. It leverages cloud-native tools and best practices to ingest stock data from the Alpha Vantage API, store it in MinIO (S3-compatible object storage), load it into Snowflake, transform it with dbt, and make it available for analytics and visualization in Metabase. The pipeline is orchestrated using Apache Airflow and is fully containerized with Docker.

---

## Tech Stack & Main Tools

- **Orchestration:** [Apache Airflow](https://airflow.apache.org/) (CeleryExecutor, Dockerized)
- **Data Extraction:** [Alpha Vantage API](https://www.alphavantage.co/)
- **Object Storage:** [MinIO](https://min.io/) (S3-compatible)
- **Data Warehouse:** [Snowflake](https://www.snowflake.com/)
- **Transformation:** [dbt (Data Build Tool)](https://www.getdbt.com/) (Snowflake & Postgres adapters)
- **Visualization:** [Metabase](https://www.metabase.com/)
- **Programming Language:** Python 3
- **Other Libraries:** pandas, numpy, boto3, requests, python-dotenv, yfinance, sqlalchemy, great-expectations, rich, tabulate

---

## Component Explanations

### `dags/`
- **get_stock.py**: Fetches daily time series data for top global stocks from Alpha Vantage, merges new data with existing MinIO JSONs, and ensures bucket existence.
- **minio_to_snowflake.py**: Reads all JSON files from MinIO, upserts them into a Snowflake RAW table using a `MERGE` statement.
- **stock_pipeline_dag.py**: Defines the Airflow DAG. Orchestrates the ETL: fetch from API → load to Snowflake → run dbt models → trigger Metabase sync.

### `dbt_finance/`
- **models/staging/**: Flattens nested JSON from RAW into tabular format.
- **models/marts/**: Builds analytics-ready tables (dimensions, facts, aggregates).
- **dbt_project.yml**: Project-level dbt config.
- **logs/, target/**: dbt run artifacts.

### `dbt_profiles/`
- **profiles.yml**: Connection settings for dbt (Snowflake, etc.).

### `config/`
- **airflow.cfg**: All Airflow settings (executor, connections, logging, email, etc.).


### Root Files
- **requirements.txt**: All Python dependencies.
- **Dockerfile**: Customizes the Airflow image (adds system libs, requirements).
- **docker-compose.yaml**: Defines all services: Airflow (web, scheduler, worker, triggerer, API), MinIO, Postgres, Redis, Metabase, Mailhog.
- **.env**: All secrets and environment variables (not committed to git).

---

## Setup & Installation

### 1. Prerequisites

- Docker & Docker Compose installed
- Python 3.8+ (for local script runs, not needed if only using Docker)
- Alpha Vantage API key
- Snowflake account & credentials

### 2. Clone the Repository

```sh
git clone <repo-url>
cd market_pulse
```

### 3. Environment Variables

Copy `.env.example` to `.env` and fill in all required secrets:

- Alpha Vantage API key
- MinIO endpoint, access/secret keys, bucket
- Snowflake credentials (user, password, account, database, schema, warehouse)
- Metabase URL, user, password, API key

### 4. Build & Start the Stack

```sh
docker-compose build
docker-compose up
```

This will start:
- Airflow (webserver, scheduler, worker, API, DAG processor)
- MinIO (S3-compatible object storage)
- Postgres (Airflow metadata DB)
- Redis (Celery broker)
- Metabase (BI dashboard)
- Mailhog (SMTP test server)

### 5. Install Python Dependencies (for local runs)

```sh
pip install -r requirements.txt
```

---

## How to Run the Pipeline

### Using Airflow (Recommended)

1. Access Airflow UI at [http://localhost:8080](http://localhost:8080) (default user: `airflow`/`airflow`).
2. Enable and trigger the `market_pulse_pipeline` DAG.
3. The DAG will:
   - Fetch stock data from Alpha Vantage and store/merge in MinIO
   - Load all MinIO JSONs into Snowflake RAW table
   - Run dbt models to transform and aggregate data
   - Trigger a schema sync in Metabase for analytics

### Running Scripts Manually

- **Fetch & Store Data:**
  ```sh
  python dags/get_stock.py
  ```
- **Load to Snowflake:**
  ```sh
  python dags/minio_to_snowflake.py
  ```
- **Run dbt Models:**
  ```sh
  cd dbt_finance
  dbt run
  ```

---

## Core Functionality & Key Modules

- **Alpha Vantage Integration:** `get_stock.py` fetches daily time series for top stocks, merges new data with existing, and stores in MinIO as JSON.
- **MinIO Integration:** All raw data is stored in MinIO, which is S3-compatible and runs locally via Docker.
- **Snowflake Integration:** `minio_to_snowflake.py` loads/merges all JSONs into a Snowflake VARIANT column for flexible schema.
- **dbt Transformations:** Staging models flatten JSON, marts build analytics tables (facts, dims, aggregates).
- **Metabase Integration:** The DAG triggers a schema sync in Metabase after dbt runs, ensuring new tables/columns are available for BI.
- **Airflow Orchestration:** The entire ETL is orchestrated as a DAG, with each step as a task.

---

## Integrations

- **Databases:** Uses Postgres for Airflow metadata, Snowflake for analytics warehouse.
- **APIs:** Alpha Vantage for stock data, Metabase API for schema sync.
- **Email/Notifications:** Uses Mailhog for local SMTP testing; Airflow is configured to send email alerts on failures.
- **Object Storage:** MinIO is used as S3-compatible storage for raw data.

---

## Requirements & Dependencies

- See [`requirements.txt`](requirements.txt) for all Python dependencies.
- Docker images: Airflow, MinIO, Postgres, Redis, Metabase, Mailhog.
- Environment variables: All secrets and endpoints must be set in `.env`.

---

## Getting Help

- For Airflow issues: [Airflow Docs](https://airflow.apache.org/docs/)
- For dbt: [dbt Docs](https://docs.getdbt.com/)
- For MinIO: [MinIO Docs](https://min.io/docs/)
- For Metabase: [Metabase Docs](https://www.metabase.com/docs/)

---

**Contributions welcome!** Please open issues or pull requests for improvements or bug fixes.
