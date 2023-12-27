import boto3
from database_utils import DatabaseConnector
from IPython.display import display
import json
import pandas as pd
import requests
from sqlalchemy import create_engine, inspect, text
import tabula



#read data from RDS database
class DataExtractor():
    def __init__(self):
        self.header_dict = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX" }
    
    #extracting the database table to a pandas datafram
    def read_rds_table(self, table_name):
        self.connector_instance = DatabaseConnector()

        #check if table name exists in AWS database if so read the table into pd DF otherwise give error
        if table_name in self.connector_instance.list_db_tables():
            self.table_name = table_name
            df = pd.read_sql_table (self.table_name,  self.connector_instance.init_db_engine())
            self.connector_instance.init_db_engine().close()
           
            return df
        
        else:
            raise FileNotFoundError("This file name is not in the AWS database, retry and remember to be case-sensitive.")
    
    #get user card details from pdf doc
    def retrieve_pdf_data(self, link):
        pdf_data = tabula.read_pdf(link, pages="all", multiple_tables=True)
        pdfs_df = pd.concat(pdf_data)
        return pdfs_df
    
    #extracting and cleaning details of each store

    #return the number of stores in the business - 451 stores
    def list_number_of_stores(self):
        endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"        

        response = requests.get(endpoint, headers = self.header_dict)

        if response.status_code == 200:
            data = response.json()
            return data["number_stores"]
        else:
            print(f"Request failed with status code: {response.status_code}, and response text: {response.text}")
    
    #extract details of each store into a dataframe
    def retrieve_stores_data(self):
        list_of_stores =[]

        for n in range(self.list_number_of_stores()):        
        
            endpoint = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{n}"
            response = requests.get(endpoint, headers = self.header_dict)

            if response.status_code == 200:
                list_of_stores.append(response.json())             
        
            else:
                print(f"Request failed with status code: {response.status_code}, and response text: {response.text}")

        return pd.json_normalize(list_of_stores)
    
    #extracting product details from aws s3 csv object into df
    def extract_from_s3(self):
        try:
            s3 = boto3.client("s3")
            response = s3.get_object(Bucket='data-handling-public', Key='products.csv')
            s3_dataframe = pd.read_csv(response.get("Body"))
            return s3_dataframe
        except Exception as e:
            print(f"Error, {e}")

    def extract_json_events_from_s3(self):
        try:
            url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
            response = requests.get(url).json()
            event_s3_df = pd.DataFrame(response)
            return event_s3_df
        except Exception as e:
            print(f"Error, {e}")





