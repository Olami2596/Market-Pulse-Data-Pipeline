from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import subprocess
import os
import requests
from dotenv import load_dotenv

# === Load environment variables ===
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(env_path)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GET_STOCK_SCRIPT = os.path.join(BASE_DIR, "get_stock.py")
MINIO_TO_SNOWFLAKE_SCRIPT = os.path.join(BASE_DIR, "minio_to_snowflake.py")


# Metabase settings
METABASE_URL = os.environ["METABASE_URL"]
METABASE_API_KEY = os.environ["METABASE_API_KEY"]

# === Helper functions ===
def run_script(script_path):
    """Run a Python script inside the Airflow environment"""
    print(f"Running {script_path}")
    result = subprocess.run(
        ["python", script_path],
        capture_output=True,
        text=True,
        check=True
    )
    print(result.stdout)
    print(result.stderr)

def trigger_metabase_sync():
    """Trigger a database sync and reanalysis in Metabase."""
    import os, requests

    METABASE_URL = os.environ["METABASE_URL"]
    METABASE_USER = os.environ["METABASE_USER"]
    METABASE_PASSWORD = os.environ["METABASE_PASSWORD"]

    login_payload = {"username": METABASE_USER, "password": METABASE_PASSWORD}
    session = requests.Session()
    login_response = session.post(f"{METABASE_URL}/api/session", json=login_payload)

    if login_response.status_code != 200:
        raise ValueError(f"Login failed: {login_response.text}")
    print("🔐 Login response:", login_response.status_code, login_response.text)

    # Get DB list
    dbs = session.get(f"{METABASE_URL}/api/database").json()
    print("📡 DB list response:", dbs)

    db_list = dbs.get("data", dbs)
    if not isinstance(db_list, list):
        raise ValueError(f"Unexpected response format: {dbs}")

    snowflake_db = next((db for db in db_list if db.get("engine") == "snowflake"), None)
    if not snowflake_db:
        raise ValueError("No Snowflake database found in Metabase")

    db_id = snowflake_db["id"]
    print(f"Found Snowflake DB ID: {db_id}")

    # Trigger schema sync
    res = session.post(f"{METABASE_URL}/api/database/{db_id}/sync_schema")
    if res.status_code in [200, 202] and "ok" in res.text.lower():
        print(f"Schema sync triggered successfully for DB ID {db_id}")
    else:
        raise ValueError(f"Failed to trigger schema sync: {res.text}")

    # Trigger fingerprint (analyzes columns, data types)
    fp = session.post(f"{METABASE_URL}/api/database/{db_id}/recompute")
    if fp.status_code in [200, 202]:
        print(f"Field fingerprinting triggered for DB ID {db_id}")
    else:
        print(f"Fingerprint trigger response: {fp.text}")

    # Trigger cache refresh (updates filters, field values, etc.)
    cache = session.post(f"{METABASE_URL}/api/database/{db_id}/rescan_values")
    if cache.status_code in [200, 202]:
        print(f"Field value cache refresh triggered for DB ID {db_id}")
    else:
        print(f"Cache refresh trigger response: {cache.text}")

    print("Metabase sync workflow completed successfully!")


# === Default DAG arguments ===

default_args = {
    'owner': 'airflow',
    'email': ['tijaniabdulhakeemola@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# === DAG Definition ===
with DAG(
    dag_id="market_pulse_pipeline",
    default_args=default_args,
    description="Stock pipeline: Alpha Vantage → MinIO → Snowflake → dbt → Metabase",
    schedule="@daily",
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=["market_pulse", "stocks", "etl", "dbt", "metabase"],
) as dag:

    # Extract from Alpha Vantage to MinIO
    fetch_from_api = PythonOperator(
        task_id="fetch_stock_data",
        python_callable=run_script,
        op_args=[GET_STOCK_SCRIPT],
    )

    # Load MinIO JSONs into Snowflake (RAW)
    load_to_snowflake = PythonOperator(
        task_id="load_minio_to_snowflake",
        python_callable=run_script,
        op_args=[MINIO_TO_SNOWFLAKE_SCRIPT],
    )

    # Run dbt models (staging + marts)
    dbt_run = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/airflow/dbt_finance && dbt run"
    )

    # Trigger Metabase schema refresh + analysis
    sync_metabase = PythonOperator(
        task_id="sync_metabase",
        python_callable=trigger_metabase_sync,
    )

    # Full DAG order
    fetch_from_api >> load_to_snowflake >> dbt_run >> sync_metabase
