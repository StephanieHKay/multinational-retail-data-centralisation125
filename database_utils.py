import yaml
import psycopg2
from sqlalchemy import create_engine, inspect, text
import pandas as pd


#class to connect to database
class DatabaseConnector():

    #read yaml with db credentials and store as dictionary
    def read_db_creds(self):        
        with open("db_creds.yaml", "r") as db_creds:
            dict_creds = yaml.safe_load(db_creds)
            return dict_creds

    #read credentials in dict and initialise adn return a sqlalchemy db engine
    def init_db_engine(self):
        credentials = self.read_db_creds()
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}")

        #will need to engine.commit() aftere very transaction 
        return engine.connect() #instead of engine.execution_options(isolation_level='AUTOCOMMIT').connect()

    #list all the tables in the database
    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        return inspector.get_table_names()

    #TODO add password
    def upload_to_db(self, df, table_name):
        self.df = df
        self.table_name = table_name
        password_db = input("database password? ")
        #local_engine = create_engine(f"postgresql+sycopg2://postgres:{password_db}@localhost:5432/Sales_Data")
        self.df.to_sql(self.table_name, local_engine, if_exists="replace")


if __name__ == "__main__":
    from data_cleaning import Datacleaning
    from data_extraction import DataExtractor
    #"All Database tables: ['legacy_store_details', 'legacy_users', 'orders_table']"
    print("All Database tables:", DatabaseConnector().list_db_tables())

    DatabaseConnector().upload_to_db(Datacleaning().clean_user_data(), "dim_users")
    print("Uploaded cleaned USER data to local DB")  

    DatabaseConnector().upload_to_db(Datacleaning().clean_card_data(), "dim_card_details") 
    print("Uploaded cleaned CARD data to local DB")  

    DatabaseConnector().upload_to_db(Datacleaning().clean_store_data(), "dim_store_details") 
    print("Uploaded cleaned STORE data to local DB")    

    DatabaseConnector().upload_to_db(Datacleaning().clean_products_data(), "dim_products") 
    print("Uploaded cleaned PRODUCT data to local DB")    

    DatabaseConnector().upload_to_db(Datacleaning().clean_orders_data(), "orders_table") 
    print("Uploaded cleaned ORDERS data to local DB")    

    DatabaseConnector().upload_to_db(Datacleaning().clean_events_data(), "dim_date_times") 
    print("Uploaded cleaned EVENTS data to local DB")    





# #testing methods- can delete later 
# test_class = DatabaseConnector()
# dict_test = test_class.read_db_creds()
# test_class.init_db_engine()
# test_class.list_db_tables()

