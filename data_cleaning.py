from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np
from dateutil.parser import parse
import re




class DataCleaning: 
    def __init__(self):                                    
        self.temp = None                                                        #### How to leave this empty without an error occuring everytiem   ####


    def clean_user_data(self , table_name):
        user_df = DataExtractor().read_rds_table(table_name)
       

        user_df['country_code'].replace({'GGB': 'GB' }, inplace=True)  # 'GGB' typo refers to united kingdom in all 6 rows, therefore replace to GB 
        user_df.replace({'NULL': np.nan , 'None': np.nan , 'NaN': np.nan}, inplace=True)  # Null values are inserted as 'NULL'. Change 'NULL' to NaN values
        user_df = user_df.dropna()  # NaN values run across entire row, so delete all NaN rows        
        user_df = user_df.drop(user_df[~user_df.country_code.isin(["GB", "DE", "US"])].index)   # All errounous values run across entire row. Using country_code column drop rows with incorrect data

        regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$' # Regular expression to match phone numbers
        user_df.loc[~user_df['phone_number'].str.match(regex_expression), 'phone_number'] = np.nan # Replace any values in phone_number column not matching regex with NaN
        # Note: have not deleted NaN in phone number as it would delete 20% of data 
        
        user_df['date_of_birth'] = user_df['date_of_birth'].apply(parse)
        user_df['date_of_birth'] = pd.to_datetime(user_df['date_of_birth'] , errors='coerce' ) # Set date_of_birth as date64 format
        user_df['join_date'] = user_df['join_date'].apply(parse)
        user_df['join_date'] = pd.to_datetime(user_df['join_date'],  errors='coerce') # Set join_date as date64 format
                                  
        user_df.drop(['index'], axis=1, inplace=True) # Drop index column
        user_df = user_df[['user_uuid', 'first_name', 'last_name', 'date_of_birth', 'phone_number', 'company', 'join_date', 'country_code']] # Reorder columns
        user_df.set_index('user_uuid', inplace=True)  # Set index to 'user_uuid' column
       
        return user_df
        
    
    def clean_card_data(self, pdf_link):
        card_details = DataExtractor().retrieve_pdf_data(pdf_link)
        
    
        card_details.replace({'NULL': np.nan, "\?": ""}, regex=True, inplace=True)  # Replace 'NULL' with np.nan values, and remove question marks. 
        card_details = card_details.dropna()   # Remove all nulls
        card_details = card_details[card_details['expiry_date'].astype(str).str.len() < 6]   # Filter dataframe to remove all erronous values across rows using expiry data column. 
        
        card_details['date_payment_confirmed'] = card_details['date_payment_confirmed'].apply(parse)
        card_details['date_payment_confirmed'] = pd.to_datetime(card_details['date_payment_confirmed'] , errors='coerce' )  # Convert to datetime64
        
        card_details.set_index('card_number', inplace=True)  # Set index to 'card_number' column
        
        return card_details
    
    
    def clean_store_data(self, store_url, api_key):
        stores_data = DataExtractor().retrieve_stores_data(store_url, api_key)

        
        stores_data = stores_data[['store_code', 'store_type', 'staff_numbers', 'address', 'locality', 'country_code', 'continent', 'latitude', 'longitude', 'opening_date']] # Redorder columns and remove 'index' and 'lat' columns 
        stores_data.set_index('store_code', inplace=True)  # Set index to store_code
        
        stores_data = stores_data.drop(stores_data[~stores_data['country_code'].isin(['GB' , 'DE', 'US'])].index) # Removes null and erroneous values which run across entire rows
        stores_data['continent'].replace({'eeEurope' : 'Europe' , 'eeAmerica': 'America'}, inplace=True)  # Replaces erronous values with correct continent
        stores_data['address'] = stores_data['address'].replace(r'\n' ,', ',regex=True)  # Replace '\n' with a comma in 'address' column 
        stores_data['staff_numbers'] = stores_data['staff_numbers'].str.replace('\D', '', regex=True) # Removes any erronous alphabet characters within values 
        stores_data = stores_data.replace({'N/A': np.nan, 'None': np.nan }) # Replace all null types to np.nan
        
        stores_data['opening_date'] = stores_data['opening_date'].apply(parse)
        stores_data['opening_date'] = pd.to_datetime(stores_data['opening_date'], errors = 'coerce') # Change 'opening_date' column Dtype to datetime64

        return stores_data


    def convert_product_weights(self, address):
        products_table = DataExtractor().extract_from_s3(address)
        

        products_table = products_table.drop(products_table[products_table['weight'].str.len() == 10].index)  # Remove erroueous values where length of string = 10 using weight column
        products_table['weight'].replace({'nan': np.nan , 'NULL': np.nan , '?': np.nan , 'NaN':np.nan})   # Replace all null types to np.nan
        products_table.dropna(inplace=True, subset='weight')  # Drop nulls in weight column

        products_table['weight'] = products_table['weight'].str.replace('ml','g')  # Convert ml to grams

         
        products_table['weight'] = products_table['weight'].apply(lambda x: re.split(r"([a-zA-Z]+)" , x))  # Split numbers and letter in weights column into a list

        def convert_weights(lists):
            if 'x' in lists:                                      # Checks for 'x' in each list, inside each row
                convert = float(lists[0]) * float(lists[2])/1000  # Convert value into kg 
                return convert
            elif 'k' in lists:                                    # Checks for 'k' in each list, inside each row
                return float(lists[0])                            # Returns value in kg 
            elif 'oz' in lists:                                   # Checks for 'oz' in each list, inside each row
                convert_oz = (float(lists[0])*0.0283495)  
                return convert_oz                                 # Convert value into kg 
            else:
                convert_grams = float(lists[0])/1000              # Convert grams into kg
                return convert_grams
            
        
        products_table['weight'] = products_table['weight'].apply(convert_weights)  # Apply convert_weights func

        products_table['weight'] = products_table['weight'].round(6)                                                   ####   MOVE .ROUND() INTO ABOVE CODE LINE ####

        return products_table
    
    def clean_products_data(self, address):
        products_table = self.convert_product_weights(address)

        
        products_table.drop(["Unnamed: 0"], axis=1 , inplace=True)  # Drop unnamed column which was equal to index 
        products_table.rename(columns={"product_price": "product_price_£", "weight": "weight_kg"}, inplace=True)  # Rename columns
        products_table = products_table[['product_code', 'product_name', 'product_price_£', 'category', 'weight_kg', 'removed','EAN', 'uuid','date_added']]  # Reorder columns 

        products_table = products_table.replace({'NULL' : np.nan , 'None' : np.nan , '?' : np.nan , 'nan' : np.nan})     #not delted? #date_details.dropna(inplace=True)
        products_table.isnull().any(axis=0)
        
        products_table['product_price_£'] = products_table['product_price_£'].map(lambda x: x.lstrip('£'))  # Remove pound symbol
        products_table['product_price_£'] = products_table['product_price_£'].astype('float64').round(2)  # Change product_price_£ column dtype 
        
        products_table['date_added'] = products_table['date_added'].apply(parse)
        products_table['date_added'] = pd.to_datetime(products_table['date_added'], errors='coerce')  # Change dtype to datetime64

        products_table['EAN'] = products_table['EAN'].astype('int64')  # Change EAN column astype to int64

        #products_table.set_index('product_code', inplace=True)                                                         ####   CHECK THIS    #####
        products_table['removed'] = products_table['removed'].replace('Still_avaliable','still_available')

        return products_table 
   
    
    def clean_orders_data(self, table_name):
        orders_table = DataExtractor().read_rds_table(table_name)

        orders_table.drop(['first_name','last_name','1', 'level_0', 'index'], axis=1, inplace=True)  # Drop unnecesary columns
        
        return orders_table
    

    def clean_date_details_data(self, address):
        date_details_table = DataExtractor().extract_from_s3(address)

         
        date_details_table = date_details_table[['date_uuid', 'year', 'month', 'day', 'time_period', 'timestamp']]  # Reorder table columns

        date_details_table.drop(date_details_table[date_details_table['day'].str.len() > 8].index, inplace= True)   # Drop all rows with erronuos columns
        date_details_table = date_details_table.replace({'NULL' : np.nan , 'None' : np.nan , '?' : np.nan , 'nan' : np.nan})  # Replace any null types with NaN
        date_details_table.dropna(inplace=True)  # Drop all nulls  

        def month_convert(x):   # Convert singular months to have zero at the start 
            if len(str(x)) == 1:
                return '0'+ str(x)
            else:
                return x

        date_details_table['month']= date_details_table['month'].apply(month_convert)  # Apply month_convert function to month column
        date_details_table.set_index('date_uuid', inplace=True)  # Set index to date_uuid 

        return date_details_table