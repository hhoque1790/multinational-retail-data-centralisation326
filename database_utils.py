import yaml
from sqlalchemy import create_engine, inspect, text
from sklearn.datasets import load_iris
import pandas as pd
# from data_cleaning import DataCleaning
class DatabaseConnector:
    """This class will be used to connect with and upload data to a database."""
    def read_db_creds(self):
        """This method reads the credentials yaml file and returns a dictionary of the credentials."""
        with open('db_creds.yaml', 'r') as file:
            db_creds = list(yaml.safe_load_all(file))
            return db_creds[0]
        
    def init_db_engine(self):
        """This method reads the credentials from the return of read_db_creds and initialises 
        and returns an sqlalchemy database engine."""
        
        db_creds=self.read_db_creds()

        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        [RDS_HOST, RDS_PASSWORD, RDS_USER, RDS_DATABASE, RDS_PORT]=list(db_creds.values())

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        # engine.connect()
        return engine
            
    def list_db_tables(self):
        """This method lists all the tables in the database allowing me to find which tables I can extract 
        data from."""
        engine=self.init_db_engine()
        inspector = inspect(engine)
        print(inspector.get_table_names())

    def read_db_tables(self, tblename="legacy_users"):
        """This method reads the data from a RDS database."""
        engine=self.init_db_engine()
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            result = conn.execute(text("SELECT * FROM "+tblename))
            for count, row in enumerate(result):
                print(row)
                if count ==5:
                    break
    
    def upload_to_db(df,tblname='dim_users'):
        db_creds=""
        with open('my_db_creds.yaml', 'r') as file:
            db_creds = list(yaml.safe_load_all(file))
            db_creds=db_creds[0]
        
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        [RDS_HOST, RDS_PASSWORD, RDS_USER, RDS_DATABASE, RDS_PORT]=list(db_creds.values())

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        engine.connect()
        df.to_sql(tblname, engine, if_exists='replace')

# DC=DatabaseConnector()
# creds=DC.list_db_tables()
# DC.read_db_tables("orders_table")
