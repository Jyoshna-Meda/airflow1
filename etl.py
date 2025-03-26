import pandas as pd
import json
import boto3
import subprocess
from io import StringIO
from minio import Minio
# MinIO Configuration
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
s3_client.list_buckets()
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
            s3_client.fget_object(BUCKET_NAME, "weather.csv", "weather.csv")
            existing_df = pd.read_csv("weather.csv")
        except Exception as e:
            print(f"{FILE_NAME} does not exist. Creating a new file.")
            existing_df = pd.DataFrame()  # Empty DataFrame if file doesn't exist

        # Convert new data to DataFrame
        new_df = pd.DataFrame(new_data)
        print(new_df)
        # Append new data to existing CSV
        updated_df = pd.concat([existing_df,new_df])
        print(updated_df)
        # Save back to MinIO
        updated_df.to_csv("weather.csv", index=False)  # Save DataFrame as a CSV file
        s3_client.fput_object(BUCKET_NAME, "weather.csv", "weather.csv")  # Upload file to MinIO
        print("Weather data updated successfully in MinIO.")

    except Exception as e:
        print(f"ETL failed: {e}")

# Run ETL process
run_etl()

