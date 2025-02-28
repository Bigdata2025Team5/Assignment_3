
import requests
import pandas as pd
import boto3
import os
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)

# AWS S3 Configuration
S3_BUCKET_NAME = "bigdata2025assignment3"
S3_FILE_NAME = "co2_daily.csv"  # The file name in S3
LOCAL_FILE_PATH = "/Users/macbookair/Desktop/Assignment_3/co2_daily.csv"  # Local file path

# NOAA CO2 Dataset URL
url = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.txt"

# Function to convert decimal year to actual date
def decimal_to_date(decimal_year):
    try:
        # Extract the year and decimal part
        year = int(decimal_year)
        decimal_part = decimal_year - year
        
        # Calculate the day of the year (fraction of 365.25 days)
        day_of_year = int(decimal_part * 365.25)
        
        # Calculate the start date (January 1st of the given year)
        start_date = datetime(year, 1, 1)
        
        # Add the calculated day of the year to the start date
        actual_date = start_date + timedelta(days=day_of_year - 1)  # day_of_year starts from 1
        
        return actual_date.strftime('%Y-%m-%d')
    except Exception as e:
        logging.error(f"Error converting decimal year {decimal_year} to date: {e}")
        return None

def fetch_and_process_data():
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data_lines = response.text.split("\n")

        data = []
        for line in data_lines:
            if not line.startswith("#") and line.strip():  # Ignore comments and empty lines
                parts = line.split()
                if len(parts) >= 5:
                    try:
                        year, month, day, decimal_year, co2_value = parts[:5]
                        
                        # Convert decimal year to actual date
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
        
        # Save DataFrame as CSV
        df.to_csv(LOCAL_FILE_PATH, index=False)
        logging.info(f"Data saved locally as {LOCAL_FILE_PATH}")
        
        return LOCAL_FILE_PATH
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from the URL: {e}")
        return None

# Upload CSV to S3 using boto3's default session
def upload_to_s3(local_file, bucket, s3_file):
   
    try:
        # Initialize the S3 client with default session (AWS credentials from environment)
        s3 = boto3.client(
        "s3",
        aws_access_key_id="AKIAZPPGAAEKCP7YN7TM",
        aws_secret_access_key="7vERWy3Zl/Gec2xRcJuIJ8rCCyJip9PuJrWqQQCe",
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



def main():
    file_path = fetch_and_process_data()
    if file_path:
        upload_to_s3(file_path)

if __name__ == "__main__":
    main()