from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import json
import boto3
from io import StringIO
import subprocess
import sys
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'minio'])
from minio import Minio
# MinIO Configuration
MINIO_ENDPOINT = "http://100.64.174.155:31444"  # Replace with your MinIO URL
ACCESS_KEY = "v4bUCLiugmSOYLPp7k8V"
SECRET_KEY = "lBQy13DD17eOCO2WPl0CMHbsLSQ4GioSK95YcWP1"
BUCKET_NAME = "minio-bucket"
FILE_NAME = "weather.csv"
 
# City for which you want weather data
city = "London"  # Change if needed
 
s3_client = Minio(
    "100.64.174.155:31444",  # Use the external IP and NodePort
    access_key="v4bUCLiugmSOYLPp7k8V",
    secret_key="lBQy13DD17eOCO2WPl0CMHbsLSQ4GioSK95YcWP1",
    secure=False
)
 
 
def fetch_weather_data():
    """Fetch hourly weather data using curl and jq."""
    curl_command = f"""
    curl -s "https://wttr.in/{city}?format=j1" | jq -r '
    .weather[0].hourly[] |
    {{
        "pressure": .pressure,
        "temparature": .tempC,
        "dewpoint": .DewPointC,
        "humidity": .humidity,
        "cloud": .cloudcover,
        "rainfall": (if .chanceofrain | tonumber > 50 then "yes" else "no" end),
        "sunshine": .chanceofsunshine,
        "winddirection": .winddirDegree,
        "windspeed": .windspeedKmph
    }}' | jq -s .
    """
    result = subprocess.run(curl_command, shell=True, capture_output=True, text=True)
 
    # Debugging output
    # Parse JSON output
    try:
        data = json.loads(result.stdout)
        return data
    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        return []
 
def run_etl():
    """Extract, transform, and load weather data into MinIO."""
    try:
        # Fetch new weather data
        new_data = fetch_weather_data()
        if not new_data:
            print("No new data fetched. Skipping update.")
            return
 
        # Download existing CSV from MinIO
        try:
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
            existing_df = pd.read_csv(response["Body"])
        except s3_client.exceptions.NoSuchKey:
            print(f"{FILE_NAME} does not exist. Creating a new file.")
            existing_df = pd.DataFrame()  # Empty DataFrame if file doesn't exist
 
        # Convert new data to DataFrame
        new_df = pd.DataFrame(new_data)
        print(new_df)
 
        # Append new data to existing CSV
        updated_df = pd.concat([existing_df, new_df])
        print(updated_df)
 
        # Save back to MinIO
        csv_buffer = StringIO()
        updated_df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=FILE_NAME,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )
        print("Weather data updated successfully in MinIO.")
 
    except Exception as e:
        print(f"ETL failed: {e}")
 
# Airflow DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 3),
    'email': ['medajyoshna28@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}
 
dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval="*/10 * * * *",  # Run every minute
    max_active_runs=1,
)
 
runs_etl = PythonOperator(
    task_id='complete_twitter_etl',
    python_callable=run_etl,
    dag=dag,
)
 
runs_etl
