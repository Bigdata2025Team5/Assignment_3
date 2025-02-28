import os
import requests
import pandas as pd
import boto3
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)

# Get the current script directory
script_directory = os.path.dirname(os.path.abspath(__file__))  # Directory of the script

# Set the file path for the CSV file to be saved in the same directory as the script
LOCAL_FILE_PATH = os.path.join(script_directory, "co2_daily.csv")  # CSV file name

# AWS S3 Configuration
S3_BUCKET_NAME = "bigdata2025assignment3"
S3_FILE_NAME = "co2_daily.csv"  # The file name in S3

# NOAA CO2 Dataset URL
url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.txt"

# Function to convert decimal year to actual date
def decimal_to_date(decimal_year):
    try:
        year = int(decimal_year)
        decimal_part = decimal_year - year
        day_of_year = int(decimal_part * 365.25)
        start_date = datetime(year, 1, 1)
        actual_date = start_date + timedelta(days=day_of_year - 1)
        return actual_date.strftime('%Y-%m-%d')
    except Exception as e:
        logging.error(f"Error converting decimal year {decimal_year} to date: {e}")
        return None

# Fetch data from the URL with error handling
try:
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for HTTP errors
    data_lines = response.text.split("\n")
except requests.exceptions.RequestException as e:
    logging.error(f"Error fetching data from the URL: {e}")
    raise

# Process the data and extract meaningful information
data = []
for line in data_lines:
    if not line.startswith("#") and line.strip():  # Ignore comments and empty lines
        parts = line.split()
        if len(parts) >= 5:
            try:
                year, month, day, decimal_year, co2_value = parts[:5]
                date = decimal_to_date(float(decimal_year))
                if date:
                    co2_value = float(co2_value)
                    data.append([date, co2_value])
                else:
                    logging.warning(f"Skipping invalid date for line: {line}")
            except ValueError:
                logging.warning(f"Skipping invalid data: {line}")

# Convert to Pandas DataFrame
df = pd.DataFrame(data, columns=["date", "co2_ppm"])

# Save DataFrame as a CSV file in the same directory as the script
df.to_csv(LOCAL_FILE_PATH, index=False)
logging.info(f"Data saved locally as {LOCAL_FILE_PATH}")

# Upload CSV to S3 using boto3's default session
def upload_to_s3(local_file, bucket, s3_file):
    try:
        # Initialize the S3 client with default session (AWS credentials from environment)
        s3 = boto3.client(
            "s3",
            aws_access_key_id="AKIAZPPGAAEKCP7YN7TM",  # Replace with your AWS credentials
            aws_secret_access_key="7vERWy3Zl/Gec2xRcJuIJ8rCCyJip9PuJrWqQQCe",  # Replace with your AWS credentials
            region_name="us-east-2",  # Change based on your AWS region
        )
        # Upload the file to S3
        s3.upload_file(local_file, bucket, s3_file)
        logging.info(f"File uploaded successfully to s3://{bucket}/{s3_file}")
    except Exception as e:
        logging.error(f"Error uploading file to S3: {e}")
        raise

# Call the function to upload
upload_to_s3(LOCAL_FILE_PATH, S3_BUCKET_NAME, S3_FILE_NAME)


