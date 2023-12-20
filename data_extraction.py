import pandas as pd
from sqlalchemy import create_engine, inspect, text
from database_utils import DatabaseConnector
from IPython.display import display
import tabula

#read data from RDS database
class DataExtractor():
    
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
    
   


