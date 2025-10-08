import requests
import time
import json
import os
import io
from datetime import datetime
import boto3
from dotenv import load_dotenv
from botocore.client import Config
from botocore.exceptions import ClientError

# --- Load environment variables ---
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(env_path)

API_KEY = os.environ["ALPHAVANTAGE_API_KEY"]
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]
BASE_URL = "https://www.alphavantage.co/query"

# --- Top 10 Global Stocks ---
TOP_10_STOCKS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "TSLA", "NVDA", "BRK.B", "V", "JPM"
]

# --- Normalize ticker quirks for Alpha Vantage ---
ALPHAVANTAGE_SYMBOLS = {
    "BRK.B": "BRK-B"
}

# --- Connect to MinIO ---
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

def ensure_bucket_exists(bucket_name):
    """Create the bucket if it doesn't exist."""
    existing_buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket_name not in existing_buckets:
        s3.create_bucket(Bucket=bucket_name)
        print(f"🪣 Created bucket: {bucket_name}")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

def fetch_stock_data(symbol):
    """Fetch full daily time series for a given symbol."""
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "full",
        "apikey": API_KEY,
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        if "Time Series (Daily)" in data:
            print(f"Retrieved {symbol} ({len(data['Time Series (Daily)'])} days)")
            return data
        else:
            print(f"No data for {symbol}: {data.get('Note') or 'Unknown issue'}")
    else:
        print(f"HTTP {response.status_code} for {symbol}")
    return None

def load_existing_from_minio(key):
    """Load existing JSON data from MinIO if available."""
    try:
        obj = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
        body = obj["Body"].read().decode("utf-8")
        return json.loads(body)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return {}
        raise

def save_incremental_to_minio(symbol, new_data):
    """Merge new Alpha Vantage data into existing MinIO JSON file."""
    key = f"alphavantage/{symbol}.json"

    existing = load_existing_from_minio(key)
    existing_series = existing.get("Time Series (Daily)", {})

    new_series = new_data.get("Time Series (Daily)", {})
    existing_series.update(new_series)

    merged_data = {
        "Meta Data": new_data.get("Meta Data", {}),
        "Time Series (Daily)": existing_series,
    }

    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=json.dumps(merged_data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"💾 Updated {symbol}: now has {len(existing_series)} days")

def main():
    ensure_bucket_exists(MINIO_BUCKET)

    for symbol in TOP_10_STOCKS:
        symbol_av = ALPHAVANTAGE_SYMBOLS.get(symbol, symbol)
        data = fetch_stock_data(symbol_av)
        if data:
            save_incremental_to_minio(symbol, data)
        time.sleep(15) 

if __name__ == "__main__":
    main()
