import yaml
from sqlalchemy import create_engine, inspect
from database_utils import DatabaseConnector
import pandas as pd
import requests
import pickle
from tqdm import tqdm
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

class DataExtractor:
    """This class will work as a utility class. Inside it, there are methods that help extract data
    from different data sources.
    The methods contained will be fit to extract data from a particular data source, these sources 
    will include CSV files, an API and an S3 bucket.
    """

    def read_rds_table(db=DatabaseConnector(),tblename="legacy_users"):
        """This method will extract a database table (legacy_users by default) to a pandas DataFrame"""
        engine=db.init_db_engine()
        users = pd.read_sql_table(tblename, engine)
        return users
    
    def retrieve_pdf_data(pdflink):
        import tabula

        dfs = tabula.read_pdf(pdflink, pages="all")
        
        result = pd.concat(dfs, ignore_index=True)

        return result

    def list_number_of_stores(no_ofstores_endpnt,headerdict): 
        response = requests.get(no_ofstores_endpnt,headers=headerdict)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Access the response data as JSON
            data = response.json()
            print("SUCCESS")
            print(data)
        # If the request was not successful, print the status code and response text
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(f"Response Text: {response.text}")

    def retrieve_stores_data(headerdict):
        storesdata=[]
        for i in tqdm(range(0,451)):
            url="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"+str(i)
            response = requests.get(url,headers=headerdict)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Access the response data as JSON
                data = response.json()
                storesdata.append(data)
            
            # If the request was not successful, print the status code and response text
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response Text: {response.text}")
        
        storesdata_df = pd.DataFrame(storesdata)
        storesdata_df.set_index('index', inplace=True)

        storesdata_df.to_pickle("storedatafile")

        # print(storesdata_df)
        # print(storesdata_df.info())
        return (storesdata_df)

    def extract_from_s3():
        s3 = boto3.client('s3')
        s3.download_file('data-handling-public', 'products.csv', 's3data/products.csv')
        productsdf = pd.read_csv("s3data/products.csv")
        # print(productsdf.loc[:,"Unnamed: 0"])
        productsdf.set_index("Unnamed: 0", inplace=True)
        return productsdf

    def extractjson_from_s3(link):
        import urllib.request, json 
        data=""
        with urllib.request.urlopen(link) as url:
            data = json.load(url)
        eventsdata=pd.DataFrame(data)
        eventsdata.to_pickle("eventsdatafile")
        return eventsdata
# DataExtractor.read_rds_table()
# print(DataExtractor.__doc__)
endpint="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
key_val={"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
# DataExtractor.list_number_of_stores(no_ofstores_endpnt=endpint,headerdict=key_val)
# DataExtractor.retrieve_stores_data(headerdict=key_val)