from database_utils import DatabaseConnector
import pandas as pd
import numpy as np
import tabula 
import yaml
import requests 

class DataExtractor:
    def __init__(self):
        self.temp= None
        self.instance = DatabaseConnector()
        
    
    def read_rds_table(self, table_name):
        instance = self.instance
        engine = instance.init_db_engine()
        table_name = pd.read_sql_table(table_name, engine)
        return table_name

    def retrieve_pdf_data(self, link):
        pdf_df_list = tabula.read_pdf(link, pages ='all', lattice=True) # outputs list of dataframes
        pdf_df=pd.DataFrame()
        for df in pdf_df_list:
            pdf_df = pd.concat([pdf_df, df], axis=0, ignore_index=True) 
        return pdf_df
    
    def list_number_of_stores(self, url , api_key):
        with open(api_key , 'r') as file:
            headers = yaml.safe_load(file) 
        response = requests.get(url, headers=headers)
        data = response.json()
        print(data) 
        
    def retrieve_stores_data(self, url,api_key):
        list_of_dicts=[]
        for store_number in range(1, 451):
            with open(api_key , 'r') as file:
                headers = yaml.safe_load(file)
                response = requests.get(f"{url}{store_number}", headers=headers)
                data=response.json()
                list_of_dicts.append(data)
        stores_data = pd.DataFrame(list_of_dicts)
        return stores_data
        


