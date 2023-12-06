import requests
import yaml
import psycopg2
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from sqlalchemy import create_engine


random.seed(100)


class AWSDBConnector:

    def __init__(self):
        pass

    def read_db_creds(self, yaml_file):
        """This reads the credentials from a yaml file
           and return a dictionary of the credentials
           The format for yaml_file is, e.g., 'db_creds.yaml'
        """
        with open(yaml_file) as credentials_yaml:
            self.credentials_dict = yaml.safe_load(credentials_yaml)
        return self.credentials_dict

    def init_engine(self, credentials_read):
        """This reads the credentials from the output of read_db_creds
           and initialise and return an sqlalchemy database engine
        """
        self.credentials = credentials_read['credentials']
        DATABASE_TYPE = self.credentials['database_type']
        DBAPI = self.credentials['dbapi']
        USER = self.credentials['user']
        PASSWORD = self.credentials['password']
        HOST = self.credentials['host']
        PORT = self.credentials['port']
        DATABASE = self.credentials['database']

        engine1 = sqlalchemy.create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?charset=utf8mb4")
        return engine1

    def data_sample(self, engine, table_name: str, row_number: int):
        """This queries the database,
           extracts the first 5 rows of data from the table
           and returns the data in dictionary format
        """

        data_query = text(f"SELECT * FROM {table_name} LIMIT {row_number}, 1")
        with self.engine.connect() as connection:
            result = connection.execute(data_query)
            return dict(next(result)._mapping)


new_connector = AWSDBConnector()
credentials_read = new_connector.read_db_creds('db_creds.yaml')
engine = new_connector.init_engine(credentials_read)
print(engine)
#sample = new_connector.data_sample(engine, "pinterest_data", 5)
#print(sample)