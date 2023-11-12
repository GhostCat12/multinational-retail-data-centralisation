from database_utils import DatabaseConnector
import pandas as pd
import numpy as np
import tabula 


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
            pdf_df = pd.concat([pdf_df, df], axis=0) 
        return pdf_df


