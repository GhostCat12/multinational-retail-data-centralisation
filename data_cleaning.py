from data_extraction import DataExtractor
import pandas as pd
import numpy as np
from dateutil.parser import parse
import re



class DataCleaning: 
    def __init__(self):
        #self.rds_table = DataExtractor().read_rds_table(table_name='legacy_users')
        #self.pdf_table = DataExtractor().retrieve_pdf_data(link='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        #self.store_table =DataExtractor().retrieve_stores_data(url='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/', api_key='api_key.yaml')
        #self.products_table =DataExtractor().extract_from_s3(address='s3://data-handling-public/products.csv')
        #self.orders_table = DataExtractor().read_rds_table('orders_table')
        self.date_Details_table = DataExtractor().extract_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

    def clean_user_data(self):
        user_df = self.rds_table
        # Clean the user data, look out for NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.
        

        # Assign index column as index and fix number order of index.                                                          # MEOW  
        user_df['index'] = range(0,15320)
        user_df.set_index('index', inplace=True)


        # DEALING WITH NULL VALUES AND INCORRECT VALUES 
        
        # This shows 21 'NULL' values and 15 instances of incorrect data
        user_df.country.value_counts() 
        # This shows 21 'NULL' values and 15 instances of incorrect data and GGB might refer to united kingdom
        user_df.country_code.value_counts() 
        
        # Check if GGB refering to united kingdom - all 6 rows show united kingdom therefore replace to GB
        user_df.loc[user_df['country_code'] == 'GGB'] 
        user_df['country_code'].replace({'GGB': 'GB' }, inplace=True)

        # Null values are inserted as 'NULL'. Change 'NULL' to NaN values
        user_df.replace({'NULL': np.nan }, inplace=True)
        # Check if all NaN values across same row - they are, so delete all NaN rows
        user_df.loc[user_df['country_code'].isna() == True]
        user_df.dropna(inplace =True)

        # To check if all incorrect values span all the way across the row by using length of uuid column - they are
        user_df['user_uuid'] = user_df['user_uuid'].astype('str')
        mask = (user_df['user_uuid'].str.len() == 10)
        check_length = user_df.loc[mask]
        # Drop rows with incorrect data
        user_df = user_df.drop(user_df[user_df.country_code.isin(["5D74J6FPFJ", "XPVCZE2L8B" , "QREF9WLI2A", "XKI9UXSCZ1" , "RVRFD92E48",                       #MEOW 
                                                                  "IM8MN1L9MJ" , "LZGTB0T5Z7" , "FB13AKRI21" , "OS2P9CMHR6", "NTCGYW8LVC", 
                                                                  "PG8MOC0UZI" , "0CU6LW3NKB" , "QVUW9JSKY3" , "VSM4IZ4EL3" , "44YAIDY048"])].index)
        
        

        # NOW CHANGING TO CORRECT D-TYPES FOR ALL COLUMNS 

        # Change first_name, last_name to string format 
        user_df['first_name'] = user_df['first_name'].astype('str')
        user_df['last_name'] = user_df['last_name'].astype('str')

        # Use phone number checker to check phone numbers follow correct expression, otherwise replace with NaN.
        regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$' # regular expression to match
        user_df.loc[~user_df['phone_number'].str.match(regex_expression), 'phone_number'] = np.nan # For every row  where the Phone column does not match our regular expression, replace the value with NaN
        # have not deleted NaN values, to delete: user_df.dropna(inplace=True)
        
        # Set date_of_birth and join_date in date64 format
        user_df['date_of_birth'] = user_df['date_of_birth'].apply(parse)
        user_df['date_of_birth'] = pd.to_datetime(user_df['date_of_birth'] , infer_datetime_format=True , errors='coerce' )
        
        user_df['join_date'] = user_df['join_date'].apply(parse)
        user_df['join_date'] = pd.to_datetime(user_df['join_date'], infer_datetime_format=True, errors='coerce')
        

        return user_df
    
    def clean_card_data(self):
        card_details = self.pdf_table
        
        # Null values are inserted as 'NULL'. Change 'NULL' to NaN values
        card_details.replace({'NULL': np.nan }, inplace=True)

        # remove all nulls
        card_details = card_details.dropna()
        
        card_provider_types = ["Diners Club / Carte Blanche", "American Express", "JCB 16 digit", "JCB 15 digit", 
                               "Maestro", "Mastercard", "Discover", "VISA 19 digit", "VISA 16 digit", "VISA 13 digit"]
        card_details = card_details.drop(card_details[~card_details['card_provider'].isin(card_provider_types)].index)
        

        #convert to datetime64
        card_details['date_payment_confirmed'] = card_details['date_payment_confirmed'].apply(parse)
        card_details['date_paystores_datament_confirmed'] = pd.to_datetime(card_details['date_payment_confirmed'] , errors='coerce' )
    
        #convert card_numbers that can convert to int64 , will return them as floats
        card_details['card_number'] = pd.to_numeric(card_details['card_number'], errors = 'coerce')
        #drop NaN's 
        card_details = card_details.dropna()
        #convert float to int type
        card_details['card_number'] = card_details['card_number'].astype('int64')
        
        print(card_details[['expiry_date','date_payment_confirmed']], '\n\n', card_details.info())
        return card_details
    
    
    def clean_store_data(self):
        stores_data = self.store_table
        
        #stores_data['index'] = range(1,451)
        stores_data.set_index('index', inplace =True)                                                                                                                                                                   #meow

        # Reorder columns
        stores_data = stores_data[['store_code', 'store_type', 'staff_numbers', 'address', 'locality', 'country_code', 'continent', 'latitude', 'longitude', 'opening_date' , 'lat']]
        
        # Replace all 'NULL' and 'None' to np.nan
        stores_data.replace({'NULL': np.nan , 'None': np.nan}, inplace=True)
        stores_data['lat']
        # Drop all non-NaN erroneous values found across all rows
        stores_data = stores_data.drop(stores_data[(~stores_data['lat'].isna())].index)
        # Drop 'Lat' column
        stores_data.drop(['lat'], axis=1, inplace=True)
        # Drop all rows with NaN values
        stores_data = stores_data.dropna()

        # Replace '\n' with a comma in 'address' column 
        stores_data['address'] = stores_data['address'].replace(r'\n' ,', ',regex=True)
        # Replace mistyped continents with correct continent
        stores_data['continent'].replace({'eeEurope' : 'Europe' , 'eeAmerica': 'America'}, inplace=True)

        # Change 'staff_numbers' Dtype to int64
        stores_data['staff_numbers'] = stores_data['staff_numbers'].str.replace('\D', '', regex=True)
        stores_data['staff_numbers'] = stores_data['staff_numbers'].astype('int64')

        # Change 'latitude' and 'logitude' column dtypes to float32 and round to 3 decimal places
        stores_data['latitude'] = stores_data['latitude'].astype('float32').round(3)
        stores_data['longitude'] = stores_data['longitude'].astype('float64').round(3)

        # Change 'opening_date' column Dtype to datetime64
        stores_data['opening_date'] = stores_data['opening_date'].apply(parse)
        stores_data['opening_date'] = pd.to_datetime(stores_data['opening_date'], errors = 'coerce')

        # set imdex to number of rows 
        stores_data = stores_data.set_axis(range(1,len(stores_data)+1))
        return stores_data


    def convert_product_weights(self):
        products_table = self.products_table
        

        # get rid of erroueous values by dropping any length of string = 10.  
        products_table = products_table.drop(products_table[products_table['weight'].str.len() == 10].index)

        # convert ml to grams
        products_table['weight'] = products_table['weight'].str.replace('ml','g')
        products_table['weight'].replace({'nan': np.nan , 'NULL': np.nan , '?': np.nan , 'NaN':np.nan})

        #dropna in weight column
        products_table.dropna(inplace=True, subset='weight') 

        #split numbers and letter in weights into a list 
        products_table['weight'] = products_table['weight'].apply(lambda x: re.split(r"([a-zA-Z]+)" , x)) 

        def t(lists):
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
        products_table['weight'] = products_table['weight'].apply(t)

        products_table['weight'] = products_table['weight'].round(6)

        return products_table
    
    def clean_products_data(self):
        products_table = self.convert_product_weights()

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

        return products_table 
    
    
    def clean_orders_data(self):
        orders_table = self.orders_table

        # drop first_name, last_name , 1 , level_0 , index
        orders_table.drop(['first_name','last_name','1', 'level_0', 'index'], axis=1, inplace=True)

        return orders_table
    
    def clean_date_details_data(self):
        date_details_table = self.date_Details_table

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

        #convert timestamp to datetime[64]
        date_details_table['timestamp'] = pd.to_datetime(date_details_table['timestamp'], format = '%H:%M:%S')
        date_details_table['timestamp'].info()

        #Convert singular months to have zero at the start 
        def month_convert(x):
            if len(str(x)) == 1:
                return '0'+ str(x)
            else:
                return x

        date_details_table['month']= date_details_table['month'].apply(month_convert)

        #convert year to datetime64
        date_details_table['year'] = pd.to_datetime(date_details_table['year'])

        return date_details_table
        









