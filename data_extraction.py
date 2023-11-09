from database_utils import DatabaseConnector
import pandas as pd
import numpy as np


class DataExtractor:
    def __init__(self, ):
        self.temp= None
        self.instance = DatabaseConnector()
        #self.table_name =
    
    def read_rds_table(self, table_name):
        instance = self.instance
        engine = instance.init_db_engine()
        table_name = pd.read_sql_table(table_name, engine)
        return table_name
