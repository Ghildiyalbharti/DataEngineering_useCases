

#Imports
import functions_framework
import json # parsing JSON data.
import requests as req
import gspread_pandas # for working with google sheets using pandas
from google.cloud import secretmanager #to access secrets using Google Cloud Secret Manager
import pandas as pd
import pandas_gbq #functions for working with Google BigQuery in combination with Pandas.


# Cloud Function 
@functions_framework.http
def hello_http(request):
    message = 'Function executed Successfully'
    
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    
    # Build the resource name of the secret version.
    project_id = '414888653736'
    secret_id = 'sa_cred'
    version_id = '1'
    name = "projects/{}/secrets/{}/versions/{}".format(project_id,secret_id,version_id)
    
    # Access the secret version.
    response = client.access_secret_version(request={"name": name})
    
    # Print the secret payload.
    # snippet is showing how to access the secret material.
    payload = response.payload.data.decode("UTF-8")
    
    # convert secret value into json format
    credentials = json.loads(payload)
	
    # Defining scopes for gsheet and gdrive APIs
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    # Access gsheet into gspread_pandas varaible
    google_sheet_file_1 = gspread_pandas.Spread('1GmKAaZQS-sLaQRmMzNXaFt6lXOkQbvxGNx2_c3NnoVk', config=credentials)
    
    # Convert into pandas dataframe
    df = google_sheet_file_1.sheet_to_df(header_rows=1).astype(str)
    df.reset_index(inplace=True)
    
    # Write values into Bigquery Table with append mode
    df.to_gbq('gcp_dataeng_demos.sa_sheet_to_bq',
             'gcp-dataeng-demos-383407',
              chunksize=10000, 
              if_exists='append'
                )
    print("Data loaded successfully")
    return message