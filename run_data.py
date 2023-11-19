from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

#2
clean = DataCleaning()
#cleaned_user_data = clean.clean_user_data('legacy_users')
#cleaned_card_details = clean.clean_card_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
#cleaned_store_data = clean.clean_store_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/', api_key='api_key.yaml')
#convert_weights = clean.convert_product_weights('s3://data-handling-public/products.csv')
#cleaned_product_data = clean.clean_products_data('s3://data-handling-public/products.csv')
#cleaned_orders_table = clean.clean_orders_data('orders_table')
clean.change_data_types()
#cleaned_date_details_table = clean.clean_date_details_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')


#3
connect = DatabaseConnector()
#connect.upload_to_db(pandas_df=cleaned_user_data, table_name ='dim_users')
#connect.upload_to_db(pandas_df=cleaned_card_details, table_name ='dim_cards_details')
#connect.upload_to_db(pandas_df=cleaned_store_data, table_name ='dim_store_details')
#connect.upload_to_db(pandas_df=cleaned_product_data, table_name ='dim_products')
#connect.upload_to_db(pandas_df=cleaned_orders_table, table_name ='orders_table')
#connect.upload_to_db(pandas_df=cleaned_date_details_table, table_name ='dim_date_times')
#connect.list_db_tables()


#1
extract = DataExtractor()
#extract.read_rds_table('orders_table')
#extract.read_rds_table('legacy_store_details')
#extract.read_rds_table('orders_table')


#link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
#extract.retrieve_pdf_data(link)

#number_of_stores_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
#extract.list_number_of_stores(number_of_stores_url, 'api_key.yaml')

#store_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
#extract.retrieve_stores_data(store_url, 'api_key.yaml')

#address1 = 's3://data-handling-public/products.csv'
#extract.extract_from_s3(address1)

#address2 = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
#extract.extract_from_s3(address=address2)





