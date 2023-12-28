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
        self.df = DataExtractor().read_rds_table("legacy_users")    
        
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
        
        #reset index
        data_frame.reset_index(inplace=True, drop=True)
        
        return data_frame
    
    #clean the store API data
    def clean_store_data(self):
        #get data from api
        store_data = DataExtractor().retrieve_stores_data()

        #correct continent eeEurope and eeAmerica
        store_data.continent = store_data.continent.str.replace("eeEurope", "Europe")
        store_data.continent = store_data.continent.str.replace("eeAmerica", "America")

        #drop the duplicate "lat" column
        store_data = store_data.drop("lat", axis=1)

        #change longititude, latitude and staff_number to numeric
        store_data.longitude = pd.to_numeric(store_data.longitude, errors='coerce')

        store_data.latitude = pd.to_numeric(store_data.latitude, errors='coerce')

        store_data.staff_numbers = pd.to_numeric(store_data.staff_numbers, errors='coerce')

        #uniform opening dates
        store_data.opening_date = pd.to_datetime(store_data.opening_date, infer_datetime_format=True, errors='coerce')

        #change null and n/a to na
        store_data = store_data.replace("NULL", np.nan)
      
        #drop nas
        store_data = store_data.dropna(axis=0, how="any")
        store_data = store_data.dropna(subset=['staff_numbers', 'longitude', 'opening_date', 'latitude'], how='all')       

        #have only one index column
        store_data.set_index("index", inplace=True)

        return store_data
    
    def convert_product_weights(self):
        self.product_dataframe = DataExtractor().extract_from_s3()
        #print(f"original weights: {self.product_dataframe.weight}")
        self.product_dataframe.weight = self.product_dataframe.weight.apply(self.gram_getter)
        return self.product_dataframe

    #coverting grams, mls,oz into kg without word kg
    def gram_getter(self,weight):    
        if "x" in str(weight):
            split_weight = weight.split("x")
            amount= int(split_weight[0].strip())
            if split_weight[1].strip().endswith("g"):
                indiv_weight = float(split_weight[1].replace("g", ""))/1000
            elif split_weight[1].strip().endswith("kg"):
                indiv_weight = float(split_weight[1].replace("kg", ""))
    
            weight = amount * indiv_weight
            return weight

        elif str(weight).endswith("kg"):
            weight = float(weight.replace("kg", ""))
            return weight

        elif str(weight).endswith("ml"):        
            weight = float(weight.replace("ml", ""))/1000
            return weight
        
        elif str(weight).endswith("g"):
            weight = float(weight.replace("g", ""))/1000
            return weight
        elif str(weight).endswith("oz"):
            weight = float(weight.replace("oz", ""))* 0.02835
            return weight
        
        else:
            return np.nan     

    #clean the rest of the products dataframe
    def clean_products_data(self):
        self.product_df = self.convert_product_weights()
        
        #drop unnamed column
        self.product_df =  self.product_df.drop("Unnamed: 0", axis=1)
      

        #turn price into number
        self.product_df.product_price = self.product_df.product_price.str.replace("£", "")
        
        self.product_df.product_price = pd.to_numeric(self.product_df.product_price, errors='coerce')

        #make date uniform
        self.product_df.date_added = pd.to_datetime(self.product_df.date_added, infer_datetime_format=True, errors="coerce")


        #drop nas
        self.product_df =  self.product_df.dropna(axis=0, how="any")
        
        return self.product_df

    def clean_orders_data(self):
        self.orders_df = DataExtractor().read_rds_table("orders_table")
        self.orders_df = self.orders_df.drop(columns = ["level_0", "first_name", "last_name", "1"], axis=1)
        self.orders_df.set_index("index", drop=True, inplace=True)
        self.orders_df.dropna(how='any',inplace= True)
        return self.orders_df
    
    def clean_events_data(self):
        self.events_df = DataExtractor().extract_json_events_from_s3()        

        #change month, day and year to numbers
        self.events_df.month = pd.to_numeric(self.events_df.month, errors='coerce')
        self.events_df.year = pd.to_numeric(self.events_df.year, errors='coerce')
        self.events_df.day = pd.to_numeric(self.events_df.day, errors='coerce')

        #make new datetime column
        self.events_df["date_time"] = pd.to_datetime(self.events_df[["year", "month", "day"]], errors ="coerce")
        self.events_df["date_time"] = pd.to_datetime(self.events_df["date_time"].astype(str) +" "+ self.events_df["timestamp"].astype(str), errors ="coerce")

        #drop na
        self.events_df = self.events_df.dropna(axis=0, how="any")          
            
        return self.events_df




if __name__== "__main__":
    main()

   

  

        
        


  
