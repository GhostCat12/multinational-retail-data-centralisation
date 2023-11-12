from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from data_extraction import DataExtractor


clean = DataCleaning()
#cleaned = clean.clean_user_data()


connect = DatabaseConnector()
#connect.upload_to_db(pandas_df=cleaned, table_name ='dim_users')

link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

extract = DataExtractor()
extract.retrieve_pdf_data(link)


