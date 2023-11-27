import pandas as pd
import tabula 
import yaml
import requests 
from concurrent.futures import ThreadPoolExecutor


class DataExtractor:

    """
    A class for extracting data from various sources.

    Attributes
    ----------
    instance : DatabaseConnector
        An instance of the DatabaseConnector for database connection.

    Methods
    -------
    read_rds_table(table_name)
        Reads a table from the RDS database.

    retrieve_pdf_data(pdf_link)
        Retrieves data from a PDF file.

    list_number_of_stores(url, api_key)
        Retrieves the number of stores from an API.

    fetch_url(url)
        Fetches data from a given URL.

    retrieve_stores_data(url, api_key, number_of_stores)
        Retrieves store data from multiple URLs concurrently.

    extract_from_s3(address)
        Extracts data from an S3 bucket based on the file format.
    """

    def __init__(self, connect):
        """
        Initializes the DataExtractor instance.

        Parameters
        ----------
        connect : DatabaseConnector
            The DatabaseConnector instance for database connection.
        """
        
        self.instance = connect
    

    def read_rds_table(self, table_name):
        """
        Reads a table from the RDS database.

        Parameters
        ----------
        table_name : str
            The name of the table to be read.

        Returns
        -------
        pandas.DataFrame
            The DataFrame containing the data from the specified table.
        """

        table_name = pd.read_sql_table(table_name, self.instance.rds_engine)
        return table_name


    def retrieve_pdf_data(self, pdf_link):
        """
        Retrieves data from a PDF file.

        Parameters
        ----------
        pdf_link : str
            The link to the PDF file.

        Returns
        -------
        pandas.DataFrame
            The DataFrame containing the data extracted from the PDF file.
        """

        link_read = tabula.read_pdf(pdf_link, pages='all')
        link_df = pd.concat(link_read)
        return link_df
    

    def list_number_of_stores(self, url, api_key):
        """
        Retrieves the number of stores from an API.

        Parameters
        ----------
        url : str
            The URL of the API endpoint.
        api_key : str
            The API key for authentication.

        Returns
        -------
        dict
            A dictionary containing the retrieved data.
        """

        with open(api_key, 'r') as file:
            headers = yaml.safe_load(file) 
        response = requests.get(url, headers=headers)
        data = response.json()
        return data 
    
    def fetch_url(self, url):
        """
        Fetches data from a given URL.

        Parameters
        ----------
        url : str
            The URL to fetch data from.

        Returns
        -------
        dict
            A dictionary containing the fetched data.
        """

        response = requests.get(url, headers=self.headers)
        data=response.json()
        return data
        
    def retrieve_stores_data(self, url, api_key, number_of_stores):
        """
        Retrieves store data from multiple URLs concurrently.

        Parameters
        ----------
        url : str
            The base URL for store data.
        api_key : str
            The API key for authentication.
        number_of_stores : dict
            A dictionary containing the number of stores to retrieve.

        Returns
        -------
        pandas.DataFrame
            The DataFrame containing the retrieved store data.
        """
        
        list_of_dicts=[]        

        pool = ThreadPoolExecutor(max_workers=2) # runs two threads at a time

        with open(api_key, 'r') as file:
            self.headers = yaml.safe_load(file)

            store_urls = [f"{url}{store_number}" for store_number in range(0, number_of_stores['number_stores'])]
            # runs self.fetch_url for every item in store_urls, uses any available thread
            for store in pool.map(self.fetch_url, store_urls):
                list_of_dicts.append(store)         
            
        stores_data = pd.DataFrame(list_of_dicts)
        return stores_data
    
    def extract_from_s3(self, address):
        """
        Extracts data from an S3 bucket based on the file format.

        Parameters
        ----------
        address : str
            The address (S3 file path) to extract data from.

        Returns
        -------
        pandas.DataFrame
            The DataFrame containing the extracted data.
        """

        if address[-3:] == 'csv':
            products_data = pd.read_csv(address)
            return products_data
        elif address[-4:] == 'json':
            date_details = pd.read_json(address)
            return date_details