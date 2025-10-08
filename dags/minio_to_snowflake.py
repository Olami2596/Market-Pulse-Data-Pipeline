import os
from dotenv import load_dotenv
import boto3
import json
import snowflake.connector

# Load .env variables
load_dotenv()

BUCKET = "market-raw"
PREFIX = "alphavantage"
SNOWFLAKE_TABLE = "RAW_ALPHAVANTAGE"
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = f"{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.RAW_ALPHAVANTAGE"


# ---- MinIO Setup ----
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

# ---- Snowflake Setup ----
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)
cursor = conn.cursor()

cursor.execute(f"CREATE DATABASE IF NOT EXISTS {os.getenv('SNOWFLAKE_DATABASE')}")
cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}")


# ---- Ensure table exists ----
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
    SYMBOL STRING PRIMARY KEY,
    DATA VARIANT,
    LOAD_DATE TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
""")

# ---- Processing ----
resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)

if "Contents" not in resp:
    print("No files found in MinIO")
else:
    for obj in resp["Contents"]:
        key = obj["Key"]
        print(f"📦 Processing {key}")
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        raw_json = json.loads(body)

        symbol = raw_json["Meta Data"]["2. Symbol"]
        json_str = json.dumps(raw_json)

        # Use MERGE to upsert data (update or insert)
        cursor.execute(f"""
        MERGE INTO {SNOWFLAKE_TABLE} AS target
        USING (SELECT %s AS SYMBOL, PARSE_JSON(%s) AS DATA) AS source
        ON target.SYMBOL = source.SYMBOL
        WHEN MATCHED THEN
            UPDATE SET target.DATA = source.DATA, target.LOAD_DATE = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (SYMBOL, DATA, LOAD_DATE)
            VALUES (source.SYMBOL, source.DATA, CURRENT_TIMESTAMP());
        """, (symbol, json_str))

    conn.commit()
    print("All symbols upserted into Snowflake successfully!")

cursor.close()
conn.close()
