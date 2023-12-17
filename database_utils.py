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
    def upload_to_db(self, df, table):
        self.df = df
        self.table = table
        local_engine = create_engine("postgresql+sycopg2://postgres:password@localhost:5432/Sales_Data")
        self.df.to_sql(self.table, local_engine, if_exists="replace")



        

#testing methods- can delete later 
test_class = DatabaseConnector()
dict_test = test_class.read_db_creds()
test_class.init_db_engine()
test_class.list_db_tables()

