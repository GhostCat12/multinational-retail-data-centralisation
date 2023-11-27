from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import links as url


# So we set up the database connector class once
connect = DatabaseConnector()
clean = DataCleaning()


#  Initialise database engine and list table names 
connect.init_db_engine() # explicitly init the db engine for readability
connect.list_db_tables()

# Then we create the extract class as we need the data from the connect var
extract = DataExtractor(connect)


#  Extract, clean and upload user data in database aliased dim_users
legacy_users_df = extract.read_rds_table('legacy_users') 
cleaned_user_data = clean.clean_user_data(legacy_users_df)
connect.upload_to_db(pandas_df=cleaned_user_data, table_name ='dim_users')

# Extract, clean and upload user card details in database as dim_card_details
card_details_df = extract.retrieve_pdf_data(url.card_details)
cleaned_card_details = clean.clean_card_data(card_details_df)
connect.upload_to_db(pandas_df=cleaned_card_details, table_name ='dim_card_details')

# list number of stores
number_of_stores = extract.list_number_of_stores(url.number_of_stores, 'api_key.yaml')

#  Extract, clean and upload stores_data in database as dim_store_details
stores_data_df = extract.retrieve_stores_data(url.stores_data, 'api_key.yaml', number_of_stores)
cleaned_store_data = clean.clean_store_data(stores_data_df)
connect.upload_to_db(pandas_df=cleaned_store_data, table_name='dim_store_details')

#  Extract, convert weights column, clean and upload products_data in database as dim_products
products_data_df = extract.extract_from_s3(url.products_data)
convert_weights = clean.convert_product_weights(products_data_df)
cleaned_product_data = clean.clean_products_data(products_data_df)
connect.upload_to_db(pandas_df=cleaned_product_data, table_name ='dim_products')

#  Extract, clean and upload orders_data in database as orders_table
orders_data_df = extract.read_rds_table('orders_table')
cleaned_orders_table = clean.clean_orders_data(orders_data_df)
connect.upload_to_db(pandas_df=cleaned_orders_table, table_name ='orders_table')

#  Extract, clean and upload date_details in database as dim_date_times
date_details_df = extract.extract_from_s3(url.date_details)
cleaned_date_details_table = clean.clean_date_details_data(date_details_df)
connect.upload_to_db(pandas_df=cleaned_date_details_table, table_name ='dim_date_times')
