import functions_framework
import pandas as pd
import pandas_gbq #functions for working with Google BigQuery in combination with Pandas.
@functions_framework.http   #decorator makes hello_http a Google Cloud Function that responds to HTTP requests.
def hello_http(request):
    message = 'function executed successfully'
    #read google sheet data into Pandas df and write into BQ table
    
    sheet_id = '1r9b.....' #replace with your sheet_id
    sheet_name = "Sheet1"
    url_1 = "https://docs.google.com/spreadsheets/d/{}/export?format=csv&gid={}".format(sheet_id,sheet_name) #replace the url
    #print(url_1)
    
    #reading data
    df=pd.read_csv(url_1)
    
    #writing data to BigQuery
    #df.to_gbq uploads the DataFrame to a BigQuery table.
    df.to_gbq('gcp_dataeng_demos.public_fruit_to_bq',  #dataset->gcp_dataeng_demos , table->public_fruit_to_bq
              'gcp_dataeng_demos_383407', #Google Cloud project ID. 
              chunksize = 1000, #breaks the upload into chunks of 1000 rows to handle large datasets
              if_exists = 'append' 
    print("Data loaded successfully")
    return message