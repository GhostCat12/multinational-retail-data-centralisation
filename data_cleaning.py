from data_extraction import DataExtractor
import pandas as pd
import numpy as np
import datetime
from dateutil.parser import parse


class DataCleaning: 
    def __init__(self):
        self.table = DataExtractor().read_rds_table(table_name='legacy_users')

    def clean_user_data(self):
        user_df = self.table
        # Clean the user data, look out for NULL values, errors with dates, incorrectly typed values and rows filled with the wrong information.
        

        # Assign index column as index and fix number order of index. 
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
        user_df = user_df.drop(user_df[user_df.country_code.isin(["5D74J6FPFJ", "XPVCZE2L8B" , "QREF9WLI2A", "XKI9UXSCZ1" , "RVRFD92E48", 
                                                                  "IM8MN1L9MJ" , "LZGTB0T5Z7" , "FB13AKRI21" , "OS2P9CMHR6", "NTCGYW8LVC", 
                                                                  "PG8MOC0UZI" , "0CU6LW3NKB" , "QVUW9JSKY3" , "VSM4IZ4EL3" , "44YAIDY048"])].index)
        
        print(user_df.head(5))

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
        print(user_df.info())

