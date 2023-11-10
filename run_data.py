from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

clean = DataCleaning()
cleaned = clean.clean_user_data()


connect = DatabaseConnector()
connect.upload_to_db(pandas_df=cleaned, table_name ='dim_users')