import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect


class DatabaseConnector:
    """
    A class for connecting to and uploading data to the database.

    Attributes
    ----------
    local_engine : sqlalchemy.engine.base.Engine or None
        The engine for the local database.
    rds_engine : sqlalchemy.engine.base.Engine or None
        The engine for the RDS database.

    Methods
    -------
    read_db_creds(cred_file)
        Reads credentials from a YAML file and returns them as a dictionary.

    init_db_engine()
        Initializes the RDS database engine using credentials.

    list_db_tables()
        Lists all tables in the connected database.

    init_local_db_engine()
        Initializes the local database engine using credentials.

    upload_to_db(pandas_df, table_name)
        Uploads a DataFrame to the local database.
    """

    def __init__(self):
        """
        Initializes the DatabaseConnector instance.
        """

        self.local_engine = None 
        self.rds_engine = None 
    
        
    def read_db_creds(self, cred_file): 
        """
        Reads credentials from a YAML file and returns them as a dictionary.

        Parameters
        ----------
        cred_file : str
            The path to the YAML file containing database credentials.

        Returns
        -------
        dict
            A dictionary containing the database credentials.
        """

        with open(cred_file, 'r') as file:
            load_as_dict = yaml.safe_load(file) 
            return load_as_dict


    def init_db_engine(self):
        """
        Initializes the RDS database engine using credentials.
        """
 
        creds = self.read_db_creds('db_creds.yaml')         
        rds_engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        self.rds_engine = rds_engine


    def list_db_tables(self):
        """
        Lists all tables in the connected database.

        Returns
        -------
        list
            A list of table names in the connected database.
        """

        inspector = inspect(self.rds_engine)
        list_of_table_names = inspector.get_table_names()
        return list_of_table_names
    
    def init_local_db_engine(self):  
        """
        Initializes the local database engine using credentials.
        """

        local_creds = self.read_db_creds('sales_data_creds.yaml')
        local_engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{local_creds['LOCAL_USER']}:{local_creds['LOCAL_PASSWORD']}@{'localhost'}:{'5432'}/{'sales_data'}")
        self.local_engine = local_engine
    

    def upload_to_db(self, pandas_df, table_name):
        """
        Uploads a DataFrame to the local database.

        Parameters
        ----------
        pandas_df : pandas.DataFrame
            The DataFrame to be uploaded.
        table_name : str
            The name of the table in the local database.
        """

        if self.local_engine is None:
            self.init_local_db_engine()

        pandas_df.to_sql(table_name, self.local_engine, if_exists='replace')
    
