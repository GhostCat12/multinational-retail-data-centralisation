import pandas as pd
import numpy as np
from dateutil.parser import parse
import re


class DataCleaning: 

    """
    A class for cleaning and preprocessing various types of data.

    Methods
    -------
    clean_user_data(user_df)
        Cleans and preprocesses user data.

    clean_card_data(card_details_df)
        Cleans and preprocesses card details data.

    clean_store_data(stores_data_df)
        Cleans and preprocesses store data.

    convert_product_weights(products_data_df)
        Converts product weights to a standardized format.

    clean_products_data(products_data_df)
        Cleans and preprocesses products data.

    clean_orders_data(orders_data_df)
        Cleans and preprocesses orders data.

    clean_date_details_data(date_details_df)
        Cleans and preprocesses date details data.
    """

    def clean_user_data(self, user_df):
        """
        Cleans and preprocesses user data.

        Parameters
        ----------
        user_df : pandas.DataFrame
            The input user data DataFrame.

        Returns
        -------
        pandas.DataFrame
            Cleaned and preprocessed user data.
        """
                
        user_df['country_code'].replace({'GGB': 'GB'}, inplace=True)  # 'GGB' typo refers to united kingdom in all 6 rows, therefore replace to GB 
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
        
    
    def clean_card_data(self, card_details_df):
        """
        Cleans and preprocesses card details data.

        Parameters
        ----------
        card_details_df : pandas.DataFrame
            The input card details data DataFrame.

        Returns
        -------
        pandas.DataFrame
            Cleaned and preprocessed card details data.
        """

        card_details_df.replace({'NULL': np.nan, "\?": ""}, regex=True, inplace=True)  # Replace 'NULL' with np.nan values, and remove question marks. 
        card_details_df = card_details_df.dropna()   # Remove all nulls
        card_details_df = card_details_df[card_details_df['expiry_date'].astype(str).str.len() < 6]   # Filter dataframe to remove all erronous values across rows using expiry data column. 
        
        card_details_df['date_payment_confirmed'] = card_details_df['date_payment_confirmed'].apply(parse)
        card_details_df['date_payment_confirmed'] = pd.to_datetime(card_details_df['date_payment_confirmed'] , errors='coerce' )  # Convert to datetime64
        
        card_details_df.set_index('card_number', inplace=True)  # Set index to 'card_number' column
        
        return card_details_df
    
    
    def clean_store_data(self, stores_data_df):
        """
        Cleans and preprocesses store data.

        Parameters
        ----------
        stores_data_df : pandas.DataFrame
            The input store data DataFrame.

        Returns
        -------
        pandas.DataFrame
            Cleaned and preprocessed store data.
        """

        stores_data_df = stores_data_df[['store_code', 'store_type', 'staff_numbers', 'address', 'locality', 'country_code', 'continent', 'latitude', 'longitude', 'opening_date']] # Redorder columns and remove 'index' and 'lat' columns 
        stores_data_df.set_index('store_code', inplace=True)  # Set index to store_code
        
        stores_data_df = stores_data_df.drop(stores_data_df[~stores_data_df['country_code'].isin(['GB' , 'DE', 'US'])].index) # Removes null and erroneous values which run across entire rows
        stores_data_df['continent'].replace({'eeEurope' : 'Europe' , 'eeAmerica': 'America'}, inplace=True)  # Replaces erronous values with correct continent
        stores_data_df['address'] = stores_data_df['address'].replace(r'\n' ,', ',regex=True)  # Replace '\n' with a comma in 'address' column 
        stores_data_df['staff_numbers'] = stores_data_df['staff_numbers'].str.replace('\D', '', regex=True) # Removes any erronous alphabet characters within values 
        stores_data_df = stores_data_df.replace({'N/A': np.nan, 'None': np.nan }) # Replace all null types to np.nan
        
        stores_data_df['opening_date'] = stores_data_df['opening_date'].apply(parse)
        stores_data_df['opening_date'] = pd.to_datetime(stores_data_df['opening_date'], errors = 'coerce') # Change 'opening_date' column Dtype to datetime64

        return stores_data_df


    def convert_product_weights(self, products_data_df):
        """
        Converts product weights to a standardized format.

        Parameters
        ----------
        products_data_df : pandas.DataFrame
            The input products data DataFrame.

        Returns
        -------
        pandas.DataFrame
            Products data with standardized weight format.
        """

        products_data_df = products_data_df.drop(products_data_df[products_data_df['weight'].str.len() == 10].index)  # Remove erroueous values where length of string = 10 using weight column
        products_data_df['weight'].replace({'nan': np.nan , 'NULL': np.nan , '?': np.nan , 'NaN':np.nan})   # Replace all null types to np.nan
        products_data_df.dropna(inplace=True, subset='weight')  # Drop nulls in weight column

        products_data_df['weight'] = products_data_df['weight'].str.replace('ml','g')  # Convert ml to grams

         
        products_data_df['weight'] = products_data_df['weight'].apply(lambda x: re.split(r"([a-zA-Z]+)" , x))  # Split numbers and letter in weights column into a list

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
            
        
        products_data_df['weight'] = products_data_df['weight'].apply(convert_weights).round(6)  # Apply convert_weights func

        return products_data_df
    
    def clean_products_data(self, products_data_df):
        """
        Cleans and preprocesses products data.

        Parameters
        ----------
        products_data_df : pandas.DataFrame
            The input products data DataFrame.

        Returns
        -------
        pandas.DataFrame
            Cleaned and preprocessed products data.
        """

        products_data_df = self.convert_product_weights(products_data_df)

        
        products_data_df.drop(["Unnamed: 0"], axis=1 , inplace=True)  # Drop unnamed column which was equal to index 
        products_data_df.rename(columns={"product_price": "product_price_£", "weight": "weight_kg"}, inplace=True)  # Rename columns
        products_data_df = products_data_df[['product_code', 'product_name', 'product_price_£', 'category', 'weight_kg', 'removed','EAN', 'uuid','date_added']]  # Reorder columns 

        products_data_df = products_data_df.replace({'NULL' : np.nan , 'None' : np.nan , '?' : np.nan , 'nan' : np.nan})     
        products_data_df.isnull().any(axis=0)  #NOTE: no nulls in all columns 
        
        products_data_df['product_price_£'] = products_data_df['product_price_£'].map(lambda x: x.lstrip('£'))  # Remove pound symbol
        products_data_df['product_price_£'] = products_data_df['product_price_£'].astype('float64').round(2)  # Change product_price_£ column dtype 
        
        products_data_df['date_added'] = products_data_df['date_added'].apply(parse)
        products_data_df['date_added'] = pd.to_datetime(products_data_df['date_added'], errors='coerce')  # Change dtype to datetime64

        products_data_df['EAN'] = products_data_df['EAN'].astype('int64')  # Change EAN column astype to int64

        products_data_df.set_index('product_code', inplace=True)                                                         
        products_data_df['removed'] = products_data_df['removed'].replace('Still_avaliable','still_available')

        return products_data_df 
   
    
    def clean_orders_data(self, orders_data_df):
        """
        Cleans and preprocesses orders data.

        Parameters
        ----------
        orders_data_df : pandas.DataFrame
            The input orders data DataFrame.

        Returns
        -------
        pandas.DataFrame
            Cleaned and preprocessed orders data.
        """

        orders_data_df.drop(['first_name','last_name','1', 'level_0', 'index'], axis=1, inplace=True)  # Drop unnecesary columns
        
        return orders_data_df
    

    def clean_date_details_data(self, date_details_df):
        """
        Cleans and preprocesses date details data.

        Parameters
        ----------
        date_details_df : pandas.DataFrame
            The input date details data DataFrame.

        Returns
        -------
        pandas.DataFrame
            Cleaned and preprocessed date details data.
        """

        date_details_df = date_details_df[['date_uuid', 'year', 'month', 'day', 'time_period', 'timestamp']]  # Reorder table columns

        bad_indexes = date_details_df.loc[lambda df: df['day'].str.len() > 3].index

        date_details_df = date_details_df.drop(bad_indexes)   # Drop all rows with erronuos columns
        date_details_df = date_details_df.replace({'NULL' : np.nan , 'None' : np.nan , '?' : np.nan , 'nan' : np.nan})  # Replace any null types with NaN
        date_details_df.dropna(inplace=True)  # Drop all nulls  

        def convert_month(x):   # Convert singular months to have zero at the start 
            if len(str(x)) == 1:
                return '0'+ str(x)
            else:
                return x

        date_details_df['month']= date_details_df['month'].apply(convert_month)  # Apply month_convert function to month column
        date_details_df.set_index('date_uuid', inplace=True)  # Set index to date_uuid 
        
        return date_details_df