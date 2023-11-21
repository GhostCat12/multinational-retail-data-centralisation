import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect


# To connect and upload data to the database
class DatabaseConnector:
    def __init__(self):
        self.temp = None    
    
    # Reads credentials yaml file and returns a dictionary of credentials 
    def read_db_creds(self): 
        with open('db_creds.yaml' , 'r') as file:
            load_as_dict = yaml.safe_load(file) 
            return load_as_dict

    # Read dictionary of credentials from read_db_credentials to initialise an sqlalchemy database engine 
    def init_db_engine(self): 
        creds = self.read_db_creds()         
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine

    # List all tables from database 
    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        list_of_table_names = inspector.get_table_names()
        print(list_of_table_names)
    
    def init_local_db_engine(self):
        with open('sales_data_creds.yaml' , 'r') as file: # yaml file containing credentials
            local_creds = yaml.safe_load(file) 
                 
        local_engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{local_creds['LOCAL_USER']}:{local_creds['LOCAL_PASSWORD']}@{'localhost'}:{'5432'}/{'sales_data'}")
        
        
        return local_engine
    
    #upload cleaaned table to database
    def upload_to_db(self, pandas_df, table_name):
        local_engine = self.init_local_db_engine()
        pandas_df.to_sql(table_name, local_engine, if_exists='replace')
    

        

# method in DatabaseConnector class called upload_to_db. This method will take in a Pandas DataFrame and table name to upload to as an argument.
# Once extracted and cleaned use the upload_to_db method to store the data in your sales_data database in a table named dim_users.
