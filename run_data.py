from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from data_extraction import DataExtractor


clean = DataCleaning()
#cleaned_user_data = clean.clean_user_data()
#cleaned_card_details = clean.clean_card_data()
#cleaned_store_data = clean.clean_store_data()
#convert_weights = clean.convert_product_weights()
cleaned_product_data = clean.clean_products_data()


## finish cleaned_product_data then put into upload db , then git add new data_cleaning method, run file updated to run new method , then extraction

connect = DatabaseConnector()
#connect.upload_to_db(pandas_df=cleaned_user_data, table_name ='dim_users')
#connect.upload_to_db(pandas_df=cleaned_card_details, table_name ='dim_cards_details')
#connect.upload_to_db(pandas_df=cleaned_store_data, table_name ='dim_store_details')
#connect.upload_to_db(pandas_df=cleaned_product_data, table_name ='dim_products')


#link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

extract = DataExtractor()
#extract.retrieve_pdf_data(link)

#number_of_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
#extract.list_number_of_stores(number_of_stores_url, 'api_key.yaml')


#store_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
#extract.retrieve_stores_data(store_url, 'api_key.yaml')

#address = 's3://data-handling-public/products.csv'
#extract.extract_from_s3(address)





