from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
import os
import boto3
from datetime import datetime

# Configurable via environment variables
LAT = float(os.getenv("WEATHER_LAT", 35.9606))   # Knoxville default
LON = float(os.getenv("WEATHER_LON", -83.9207))
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "mlops-yannick")

def fetch_weather_data(**context):
    """
    Fetch current weather from Open-Meteo API and save as CSV.
    """
    url = (
        "https://api.open-meteo.com/v1/forecast?"
        f"latitude={LAT}&longitude={LON}&current_weather=true"
    )

    response = requests.get(url, timeout=30)
    data = response.json()

    if "current_weather" not in data:
        raise ValueError(f"Invalid API response: {data}")

    cw = data["current_weather"]

    row = {
        "timestamp": datetime.utcnow().isoformat(),
        "latitude": LAT,
        "longitude": LON,
        "temperature": cw.get("temperature"),
        "windspeed": cw.get("windspeed"),
        "winddirection": cw.get("winddirection")
    }

    df = pd.DataFrame([row])
    file_name = f"/tmp/weather_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(file_name, index=False)

    context["ti"].xcom_push(key="file_path", value=file_name)
    return file_name


def upload_to_s3(**context):
    """
    Upload the CSV file to S3, creating the bucket if it does not exist.
    """
    file_path = context["ti"].xcom_pull(task_ids="fetch_weather", key="file_path")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File missing: {file_path}")

    s3 = boto3.client("s3")

    # Create bucket if it doesn't exist
    existing_buckets = [b['Name'] for b in s3.list_buckets().get('Buckets', [])]
    if S3_BUCKET_NAME not in existing_buckets:
        print(f"Bucket {S3_BUCKET_NAME} does not exist. Creating it.")
        s3.create_bucket(Bucket=S3_BUCKET_NAME)

    s3_key = f"weather/{os.path.basename(file_path)}"
    s3.upload_file(file_path, S3_BUCKET_NAME, s3_key)
    print(f"✅ Uploaded to s3://{S3_BUCKET_NAME}/{s3_key}")
    return s3_key


default_args = {
    "owner": "airflow",
    "depends_on_past": False
}

with DAG(
    dag_id="weather_to_s3",
    default_args=default_args,
    schedule_interval="*/2 * * * *",  # every 2 minutes
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "s3"]
) as dag:

    fetch_weather = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather_data,
        provide_context=True,
    )

    upload_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
    )

    fetch_weather >> upload_s3
