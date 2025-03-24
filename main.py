import google.cloud.bigquery as bigquery
import pandas
import db_dtypes
import requests
import json
import os
from google.auth import default
from google.cloud import bigquery
import utils
from datetime import datetime

# Environment Variables (Set these in Cloud Functions settings)
# KLAVIYO_API_KEY = os.getenv("KLAVIYO_API_KEY")  # Your Klaviyo private API key
# BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")  # Your GCP project ID
# BQ_DATASET = os.getenv("BQ_DATASET")  # Your BigQuery dataset
# BQ_TABLE = os.getenv("BQ_TABLE")  # Your BigQuery table name


# Klaviyo API endpoint
KLAVIYO_API_URL = "https://a.klaviyo.com/api/profiles/"
KLAVIYO_API_KEY = "Dummy"

def update_klaviyo_profiles():
    """Fetch data from BigQuery, format it, and send to Klaviyo."""
    print("Starting Klaviyo profile update process")

    # Fetch data from BigQuery
    data = utils.fetch_data_from_bq()

    if not data:
        print("No data fetched from BigQuery. Exiting process.")
        return

    # Format data for Klaviyo
    payload = utils.format_for_klaviyo(data)

    # Send data to Klaviyo
    print("Sending data to Klaviyo")
    headers = {
        "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "revision": datetime.today().strftime('%Y-%m-%d')  # Required format YYYY-MM-DD
    }

    response = requests.post(KLAVIYO_API_URL, headers=headers, data=payload)

    # Check response
    if response.status_code in [200, 202]:
        print("Successfully updated Klaviyo profiles.")
    else:
        print(f"Failed to update Klaviyo: {response.status_code} - {response.text}")

if __name__ == "__main__":
    update_klaviyo_profiles()
