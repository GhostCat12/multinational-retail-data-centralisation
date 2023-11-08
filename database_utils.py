import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect


class DatabaseConnector:
    def __init__(self):
        self.temp = None    
    
    
    def read_db_creds(self):
        with open('db_creds.yaml' , 'r') as file:
            load_as_dict = yaml.safe_load(file) 
            return load_as_dict

    def init_db_engine(self):
        creds = self.read_db_creds()         
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        table_list = inspector.get_table_names()
        return table_list

    
test = DatabaseConnector()
print(test.list_db_tables())
