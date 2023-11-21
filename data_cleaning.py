from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np
from dateutil.parser import parse
import re
from sqlalchemy import text




class DataCleaning: 
    def __init__(self):
        # how to leave this empty without an error occuring everytiem 
        self.temp = None


    def clean_user_data(self , table_name):
        user_df = DataExtractor().read_rds_table(table_name)

        
        # DEALING WITH NULL VALUES AND INCORRECT VALUES 
        
        # This shows 21 'NULL' values and 15 instances of incorrect data
        user_df.country.value_counts() 
        # This shows 21 'NULL' values and 15 instances of incorrect data and GGB might refer to united kingdom
        user_df.country_code.value_counts() 
        
        # Check if GGB refering to united kingdom - all 6 rows show united kingdom therefore replace to GB
        user_df.loc[user_df['country_code'] == 'GGB'] 
        user_df['country_code'].replace({'GGB': 'GB' }, inplace=True)

        # Null values are inserted as 'NULL'. Change 'NULL' to NaN values
        user_df.replace({'NULL': np.nan , 'None': np.nan , 'NaN': np.nan}, inplace=True)
        # Check if all NaN values across same row - they are, so delete all NaN rows
        user_df.loc[user_df['country_code'].isna() == True]
        user_df = user_df.dropna()

        # To check if all incorrect values span all the way across the row by using length of uuid column - they are
        user_df['user_uuid'] = user_df['user_uuid'].astype('str')
        mask = (user_df['user_uuid'].str.len() == 10)
        check_length = user_df.loc[mask]
        # Drop rows with incorrect data
        user_df = user_df.drop(user_df[~user_df.country_code.isin(["GB", "DE", "US"])].index)
        
        

        # NOW CHANGING TO CORRECT D-TYPES FOR ALL COLUMNS 

        # Change first_name, last_name to string format 
        user_df['first_name'] = user_df['first_name'].astype('str')
        user_df['last_name'] = user_df['last_name'].astype('str')

        # Use phone number checker to check phone numbers follow correct expression, otherwise replace with NaN.
        regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$' # regular expression to match
        user_df.loc[~user_df['phone_number'].str.match(regex_expression), 'phone_number'] = np.nan # For every row  where the Phone column does not match our regular expression, replace the value with NaN
        # have not deleted NaN in phone number as it cover 1/5 of data 
        user_df['phone_number'].isna().sum()
        
        # Set date_of_birth and join_date in date64 format
        user_df['date_of_birth'] = user_df['date_of_birth'].apply(parse)
        user_df['date_of_birth'] = pd.to_datetime(user_df['date_of_birth'] , errors='coerce' )
        
        user_df['join_date'] = user_df['join_date'].apply(parse)
        user_df['join_date'] = pd.to_datetime(user_df['join_date'],  errors='coerce')

        # drop index column                                                         
        user_df.drop(['index'], axis=1, inplace=True)

        #reorder columns
        user_df = user_df[['user_uuid', 'first_name', 'last_name', 'date_of_birth', 'phone_number', 'company', 'join_date', 'country_code']]

        user_df.set_index('user_uuid', inplace=True)
       
        return user_df
        
    
    def clean_card_data(self, pdf_link):
        card_details = DataExtractor().retrieve_pdf_data(pdf_link)
        
        # Replace 'NULL' with np.nan values, and remove question marks. 
        card_details.replace({'NULL': np.nan, "\?": ""}, regex=True, inplace=True)
        # remove all nulls
        card_details = card_details.dropna()
        # filter dataframe to remove all erronous values across rows using expiry data column. 
        card_details = card_details[card_details['expiry_date'].astype(str).str.len() < 6]
        # convert to datetime64
        card_details['date_payment_confirmed'] = card_details['date_payment_confirmed'].apply(parse)
        card_details['date_payment_confirmed'] = pd.to_datetime(card_details['date_payment_confirmed'] , errors='coerce' )
        # set index to 'card_number' column
        card_details.set_index('card_number', inplace=True)
        
        return card_details
    
    
    def clean_store_data(self, store_url, api_key):
        stores_data = DataExtractor().retrieve_stores_data(store_url, api_key)

        # Redorder columns and remove 'index' and 'lat' columns 
        stores_data = stores_data[['store_code', 'store_type', 'staff_numbers', 'address', 'locality', 'country_code', 'continent', 'latitude', 'longitude', 'opening_date']]
        # Set index to store_code
        stores_data.set_index('store_code', inplace=True)
        # Removes null and erroneous values that run across entire rows
        stores_data = stores_data.drop(stores_data[~stores_data['country_code'].isin(['GB' , 'DE', 'US'])].index)
        # Replaces erronous values with correct continent
        stores_data['continent'].replace({'eeEurope' : 'Europe' , 'eeAmerica': 'America'}, inplace=True)
        # Replace '\n' with a comma in 'address' column 
        stores_data['address'] = stores_data['address'].replace(r'\n' ,', ',regex=True)
        # removes any erronous alphabet characters within values 
        stores_data['staff_numbers'] = stores_data['staff_numbers'].str.replace('\D', '', regex=True)
        #stores_data['staff_numbers'] = stores_data['staff_numbers'].astype('int64')
        # Change 'opening_date' column Dtype to datetime64
        stores_data['opening_date'] = stores_data['opening_date'].apply(parse)
        stores_data['opening_date'] = pd.to_datetime(stores_data['opening_date'], errors = 'coerce')
        #replace N/A to np.nan
        stores_data = stores_data.replace({'N/A': np.nan, 'None': np.nan })
        return stores_data


    def convert_product_weights(self, address):
        products_table = DataExtractor().extract_from_s3(address)
        

        # get rid of erroueous values by dropping any length of string = 10.  
        products_table = products_table.drop(products_table[products_table['weight'].str.len() == 10].index)

        # convert ml to grams
        products_table['weight'] = products_table['weight'].str.replace('ml','g')
        products_table['weight'].replace({'nan': np.nan , 'NULL': np.nan , '?': np.nan , 'NaN':np.nan})

        #dropna in weight column
        products_table.dropna(inplace=True, subset='weight') 

        #split numbers and letter in weights into a list 
        products_table['weight'] = products_table['weight'].apply(lambda x: re.split(r"([a-zA-Z]+)" , x)) 

        def convert_weights(lists):
            if 'x' in lists: # checks each list inside each row
                convert = float(lists[0]) * float(lists[2])/1000 # the formula to convert into single value kg 
                return convert
            elif 'k' in lists:
                return float(lists[0])
            elif 'oz' in lists:
                convert_oz = (float(lists[0])*0.0283495)
                return convert_oz
            else:
                convert_grams = float(lists[0])/1000
                return convert_grams
            
        #apply above func
        products_table['weight'] = products_table['weight'].apply(convert_weights)

        products_table['weight'] = products_table['weight'].round(6)

        return products_table
    
    def clean_products_data(self, address):
        products_table = self.convert_product_weights(address)

        #drop unnamed column which was equal to index 
        products_table.drop(["Unnamed: 0"], axis=1 , inplace=True)                                                                                             #may have to bring this back 

        #rename
        products_table.rename(columns={"product_price": "product_price_£", "weight": "weight_kg"}, inplace=True)

        #reorder table columns 
        products_table = products_table[['product_code', 'product_name', 'product_price_£', 'category', 'weight_kg', 'removed','EAN', 'uuid','date_added']]

        # replace any nulls and delete
        products_table = products_table.replace({'NULL' : np.nan , 'None' : np.nan , '?' : np.nan , 'nan' : np.nan})                                                    #not delted? #date_details.dropna(inplace=True)
        products_table.isnull().any(axis=0)

        # check for errouns, theres no erronous
        products_table.loc[products_table['EAN'].str.len() < 11]

        #remove pound sign
        products_table['product_price_£'] = products_table['product_price_£'].map(lambda x: x.lstrip('£'))

        #change price dtype 
        products_table['product_price_£'] = products_table['product_price_£'].astype('float64').round(2)

        #check min max prices do .describe()
        products_table['product_price_£'].describe()

        # change dtype datetime64
        products_table['date_added'] = products_table['date_added'].apply(parse)
        products_table['date_added'] = pd.to_datetime(products_table['date_added'], errors='coerce')

        products_table['EAN'] = products_table['EAN'].astype('int64')

        #products_table.set_index('product_code', inplace=True)

        products_table['removed'] = products_table['removed'].replace('Still_avaliable','still_available')

        return products_table 
   
    
    def clean_orders_data(self, table_name):
        orders_table = DataExtractor().read_rds_table(table_name)

        # drop first_name, last_name , 1 , level_0 , index
        orders_table.drop(['first_name','last_name','1', 'level_0', 'index'], axis=1, inplace=True)
        return orders_table
    
    def clean_date_details_data(self, address):
        date_details_table = DataExtractor().extract_from_s3(address) # ew

        #reorder table columns 
        date_details_table = date_details_table[['date_uuid', 'year', 'month', 'day', 'time_period', 'timestamp']]

        # check for nulls and any erroneous values and check multiples columns to see all columns have 15 'NULL'
        check1 = date_details_table['month'].value_counts()
        check2 = np.sort(date_details_table['month'].unique())

        #check erroneous across entire rows  #it is
        mask1 = (date_details_table['day'].str.len() > 8)
        check_erroneous = date_details_table.loc[mask1]

        #check 'NULL' across all rows       #it is
        mask2 = (date_details_table['day'].str.len() == 4)
        check_NULL = date_details_table.loc[mask2]

        #drop all rows with erronuos columns
        date_details_table.drop(date_details_table[date_details_table['day'].str.len() > 8].index, inplace= True)

        # replace any nulls and delete
        date_details_table = date_details_table.replace({'NULL' : np.nan , 'None' : np.nan , '?' : np.nan , 'nan' : np.nan})
        date_details_table.dropna(inplace=True)
        date_details_table.isnull().any(axis=0)


        #Convert singular months to have zero at the start 
        def month_convert(x):
            if len(str(x)) == 1:
                return '0'+ str(x)
            else:
                return x

        date_details_table['month']= date_details_table['month'].apply(month_convert)
        date_details_table.set_index('date_uuid', inplace=True)

        return date_details_table
    
    

