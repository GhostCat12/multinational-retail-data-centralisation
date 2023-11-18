
# Multinational Retail Data Centralisation
    
    
## Table of Contents:



## A description of the project: 
makes sales data accessible from one centralised location for a multinational company that sells various goods across the globe
    
    

## The aim of the project: 
is to gather data from differnt reasources, clean all data and develop a star based schema database in-which important metrics can be queried on from one centralised location 
    
    

## what you learned:



## Installation instructions:
    conda eviroment 



## Usage instructions:
run_data.py



## File structure of the project:

#### 1. data_cleaning.py  
Contains 'DataCleaning' class holding the following methods to clean the data:
- clean_user_data(self)
- clean_card_data(self)
- clean_store_data(self)
- convert_product_weights(self)
- clean_products_data(self)       

#### 2. data_extraction.py
Contains 'DataExtractor' class holding the following methods to extract the data:
- read_rds_table(self, table_name)
- retrieve_pdf_data(self, link)
- list_number_of_stores(self, url , api_key)
- retrieve_stores_data(self, url,api_key)
- extract_from_s3(self, address)

#### 3. database_utils.py  
Contains 'DatabaseConnector' class holding the folling methods to connect the data:
- read_db_creds(self)
- init_db_engine(self)
- list_db_tables(self)
- upload_to_db(self, pandas_df, table_name)
            
#### 4. run_data.py  
Contains code to run differnt methods from differnt classes within different files in one location 



## License information
