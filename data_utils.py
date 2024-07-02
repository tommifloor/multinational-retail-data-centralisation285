# Script to connect and upload data to database 
import psycopg2
import yaml

from sqlalchemy import create_engine
from sqlalchemy import inspect
from urllib.parse import quote_plus

class DatabaseConnector:
    """
    A class to connect and upload data to database.

    Parameters:
    -----------

    Attributes:
    -----------

    Methods:
    --------
    read_db_cred(db_creds_file)
        Reads database credentials from YAML file. Returns dictionary
        of database credentials.
    init_db_engine(db_creds)
        Connects to database to initialize and return SQL Alchemy
        database engine.
    list_db_tables(engine)
        Lists database tables.
    """
    def __init__(self, download_creds=None, upload_creds=None):

        self.download_creds = download_creds if upload_creds is not None else 'credentials/rds_creds.yaml'
        self.upload_creds = upload_creds if upload_creds is not None else 'credentials/pg_creds.yaml'
        self.engine = None

    # Methods
    def read_db_creds(self, creds):
        with open(creds, 'r') as db_creds_file:
            db_creds = yaml.safe_load(db_creds_file)
        return db_creds
    
    def init_db_engine(self, db_creds=None):
        # Read database credentials
        if db_creds == None:
            db_creds = self.read_db_creds(self.download_creds)
        else:
            db_creds = self.read_db_creds(db_creds)
        
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        ENDPOINT = db_creds['HOST']
        USER = db_creds['USER']
        PASSWORD = quote_plus(db_creds['PASSWORD'])
        PORT = db_creds['PORT']
        DATABASE = db_creds['DATABASE']
        # Initialize SQL Alchemy engine
        engine = create_engine(
            f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
            )
        self.engine = engine
        return engine
    
    def list_db_tables(self):
        table_names = inspect(self.engine).get_table_names()
        return table_names
    
    def upload_to_db(self, pd_dataframe, table_name, db_creds=None):
        if db_creds == None:
            engine = self.init_db_engine(self.upload_creds)
        else:
            engine = self.init_db_engine(db_creds)
        #try:
        pd_dataframe.to_sql(table_name, engine, if_exists='replace')
        print("Dataframe uploaded")
        # except:


if __name__ == "__main__":
    cxn = DatabaseConnector()
    print("cxn initialized")
