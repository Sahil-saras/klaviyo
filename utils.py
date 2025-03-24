import pandas as pd
from google.cloud import bigquery, secretmanager
from google.oauth2 import service_account
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
import json

def get_service_account_creds_from_secret(secret_id, project_id):
    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": secret_name})
    service_account_info = json.loads(response.payload.data.decode("UTF-8"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    return credentials


def fetch_data_from_bq():
    """Fetch data from BigQuery and return as a list of dictionaries."""
    print("Starting BigQuery data fetch...")
    
    get_service_account_creds_from_secret("gcs-signer-key", project_id)
    client = storage.Client(credentials=credentials, project=project_id)

    client = bigquery.Client.from_service_account_json(service_account_file)

    query = """
        SELECT profile_id,
               email,
               segment_36Mspend_level_1,
               segment_36Mspend_level_2,
               recent_purchase_flag,
               at_risk_flag,
               customer_gender,
               platform_type
        FROM `fluted-union-400300.faherty_dev_presentation.klaviyo_segments`
        where profile_id = 'MsdPKc'
    """

    query_job = client.query(query)
    results = query_job.result()
    
    data = [dict(row) for row in results]
    
    print(f"Fetched {len(data)} records from BigQuery")
    return data

def format_for_klaviyo(data):
    """Format BigQuery data into JSON format for Klaviyo API."""
    print("Formatting data for Klaviyo...")

    formatted_profiles = []

    for row in data:
        profile = {
            "profile_id": row["profile_id"],
            "email": row["email"],
            "custom_attributes": {
                "segment_36Mspend_level_1": row["segment_36Mspend_level_1"],
                "segment_36Mspend_level_2": row["segment_36Mspend_level_2"],
                "recent_purchase_flag": row["recent_purchase_flag"],
                "at_risk_flag": row["at_risk_flag"],
                "customer_gender": row["customer_gender"],
                "platform_type": row["platform_type"],
            }
        }
        formatted_profiles.append(profile)

    print(f"Formatted {len(formatted_profiles)} profiles for Klaviyo")
    return json.dumps({"profiles": formatted_profiles}, indent=4)


# if __name__ == "__main__":
#     data = fetch_data_from_bq()  # Fetch data
#     formatted_json = format_for_klaviyo(data)  # Format data
#     print(formatted_json)  # Print formatted JSON output

