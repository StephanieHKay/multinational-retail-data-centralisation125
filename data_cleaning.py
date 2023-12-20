from data_extraction import DataExtractor
from sqlalchemy import create_engine, inspect, text

from IPython.display import display
import pandas as pd
import numpy as np
import re
import tabula

class Datacleaning():
           
    
    #cleans null values, errors with dates, incorrecrtly typed values and rows filled with the wrong information
    def clean_user_data(self):
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
        
        return self.df
        
    #TODO clean phone numbers to more uniform


    #clean card data to remove any erroneous values, null values or error with formatting
    def clean_card_data(self):
        data_frame = DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        
        #payment dates muddled formats make more uniform
        data_frame.date_payment_confirmed =pd.to_datetime(data_frame.date_payment_confirmed, infer_datetime_format=True, errors="coerce")

        #convert null to na
        data_frame = data_frame.replace("NULL", np.nan)

         #drop na rows
        data_frame = data_frame.dropna(axis=0, how="any")

        #card numbers with question marks
        data_frame.card_number=data_frame.card_number.astype(str)
        data_frame.card_number = data_frame.card_number.str.replace("?", "")
        #data_frame.card_number=data_frame.card_number.astype(int)

        #expire dates incorrect    
        #reset index
        data_frame.reset_index(inplace=True, drop=True)
        
        return data_frame



# #card testing can delete later
# data = Datacleaning().clean_card_data()
# display(data.head())


# pdfs_df = pd.concat(test_extracted)
# pd.set_option("display.max_columns", None)
# display(pdfs_df[11320:11340])
# cleaned_df = Datacleaning()
# print("\n cleaned:", cleaned_df.clean_card_data(pdfs_df[11320:11340]))





   
        
              

                

  

        
        


  


# #testing
# orders_table = (Datacleaning().clean_user_data("legacy_users"))
# pd.set_option("display.max_columns", None)
# display(orders_table.head(10))
# #display(orders_table.info())
# # display(orders_table.groupby(["country_code"]).groups)
# #display(orders_table.describe())

# count=0
# for num in orders_table["phone_number"]:
#      pattern = r'^(?!.*\s)(?!.{10,11}$)[0-9,./a-zA-Z]+$'
#      if not re.match (pattern, num):
#         print(num)
#         count+=1
# print(count)
