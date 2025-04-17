import pandas as pd
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess
import sys

# Install MinIO package
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'minio'])

# MinIO Configuration
from minio import Minio

MINIO_ENDPOINT = "100.64.174.155:31444"  # Replace with your MinIO URL
ACCESS_KEY = "v4bUCLiugmSOYLPp7k8V"
SECRET_KEY = "lBQy13DD17eOCO2WPl0CMHbsLSQ4GioSK95YcWP1"
BUCKET_NAME = "minio-bucket"
FILE_NAME = "weather.csv"

# City for which you want weather data
city = "London"  # Change if needed

# Initialize MinIO (S3) client
s3_client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

def fetch_weather_data():
    """Fetch hourly weather data using curl."""
    curl_command = 'curl -s "https://wttr.in/London?format=j1"'

    # Run curl command and capture output
    result = subprocess.run(curl_command, shell=True, capture_output=True, text=True)

    try:
        # Load JSON response
        data = json.loads(result.stdout)

        # Extract required fields from JSON
        hourly_data = data["weather"][0]["hourly"]

        processed_data = []
        for entry in hourly_data:
            processed_data.append({
                "pressure": entry["pressure"],
                "temparature": entry["tempC"],  # Fixed typo
                "dewpoint": entry["DewPointC"],
                "humidity": entry["humidity"],
                "cloud": entry["cloudcover"],
                "rainfall": "yes" if int(entry["chanceofrain"]) > 50 else "no",
                "sunshine": entry["chanceofsunshine"],
                "winddirection": entry["winddirDegree"],
                "windspeed": entry["windspeedKmph"]
            })

        # Print JSON output (equivalent to jq)
        print(json.dumps(processed_data, indent=2))

        return processed_data

    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        return []

def run_etl():
    """Extract, transform, and load weather data into MinIO."""
    try:
        # Fetch new weather data
        data = fetch_weather_data()

        if not data:
            print("No new data fetched.")
            return

        # Download existing CSV from MinIO
        try:
            s3_client.fget_object(BUCKET_NAME, FILE_NAME, FILE_NAME)
            existing_df = pd.read_csv(FILE_NAME)
        except Exception:
            print(f"{FILE_NAME} does not exist. Creating a new file.")
            existing_df = pd.DataFrame()  # Empty DataFrame if file doesn't exist

        # Convert new data to DataFrame
        new_df = pd.DataFrame(data)
        print(new_df)

        # Append new data to existing CSV
        updated_df = pd.concat([existing_df, new_df])
        print(updated_df)

        # Save back to MinIO
        updated_df.to_csv(FILE_NAME, index=False)  # Save DataFrame as a CSV file
        s3_client.fput_object(BUCKET_NAME, FILE_NAME, FILE_NAME)  # Upload file to MinIO
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
    'weather_etl_dag',
    default_args=default_args,
    description='DAG to fetch weather data and store it in MinIO',
    schedule_interval="0 */12 * * *",
)

run_weather_etl = PythonOperator(
    task_id='weather_etl_task',
    python_callable=run_etl,
    dag=dag,
)
