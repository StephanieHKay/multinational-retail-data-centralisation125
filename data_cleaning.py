from data_extraction import DataExtractor
from sqlalchemy import create_engine, inspect, text
from database_utils import DatabaseConnector
from IPython.display import display
import pandas as pd
import numpy as np
import re

class Datacleaning():
    
    #cleans null values, errors with dates, incorrecrtly typed values and rows filled with the wrong information
    def clean_user_data(self, table_name):
        #instantiate the datafram
        self.df = DataExtractor.read_rds_table(self, table_name)    
        
        #set one index column
        self.df.set_index("index", inplace=True)

        #convert date_of_birth and join_date to datetime 
        self.df.date_of_birth =pd.to_datetime(self.df.date_of_birth, infer_datetime_format=True, errors="coerce")
        self.df.join_date =pd.to_datetime(self.df.join_date, infer_datetime_format=True, errors="coerce")
        

        #drop null rows once coverted to recongisable null
        self.df = self.df.replace("NULL", np.nan)
        self.df = self.df.dropna(axis=0, how="any")

        #groupings show GGB,GB,DE,US country codes but only US,UK,Denmark countries
        #convert GBB to GB
        self.df.replace("GGB", "GB", inplace=True)

        #clean 42 email addresses that do not match due to doube/@@
        for email in self.df.email_address:
            if not re.match((r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"), email) and "@@" in email:
                self.df.email_address = self.df.email_address.str.replace("@@", "@")
        
        # clean phone numbers to more uniform
   
        
              

            
        

  

        return self.df
        


    # def data_type_to_numberic(self, column):
    #     self.column = column
    #     try:
    #         self.column = self.column.pd.astype("int64")
    #         return self.column
    #     except ValueError as e:
    #         print(f"{e}")
    #         pass

    #clean orders_table 11 columns
        #"level_0" and "index" columns are duplicate - delete one and other index
        #first name and last names have 105K "None" values
        # colum "1" is full of null - remove

    #clean legacy_users - 12 columns all non-null
                
        #there are invalid addresses without postcodes and street names
        # phone number formts are mudled
    
    #clean legacy_store_details
        #lat only has 11 non null entries
        # lat vs latitiude
        #some addresses are invaluie with n/a for example
        #remove where majority of rows say na - web stores for example
        #change logitude and latitiude to float
        #change opening date to tadetime from yyyy-mm-dd sometimes and oct 2012 08 others
        #change staff number to int


#testing
orders_table = (Datacleaning().clean_user_data("legacy_users"))
pd.set_option("display.max_columns", None)
display(orders_table.head(10))
#display(orders_table.info())
# display(orders_table.groupby(["country_code"]).groups)
#display(orders_table.describe())

count=0
for num in orders_table["phone_number"]:
     pattern = r'^(?!.*\s)(?!.{10,11}$)[0-9,./a-zA-Z]+$'
     if not re.match (pattern, num):
        print(num)
        count+=1
print(count)
