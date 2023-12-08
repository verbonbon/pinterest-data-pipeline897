import requests
import yaml
from time import sleep
import random
from multiprocessing import Process
import sqlalchemy
from sqlalchemy import text
from sqlalchemy import create_engine
import json
from time import sleep
import random
from multiprocessing import Process
import boto3
import datetime

random.seed(100)


class AWSDBConnector:

    def __init__(self):
        pass

    def read_creds(self, yaml_file):
        """This reads the credentials from a yaml file
           and return a dictionary of the credentials
           The format for yaml_file is, e.g., 'db_creds.yaml'
        """
        with open(yaml_file) as credentials_yaml:
            self.credentials_dict = yaml.safe_load(credentials_yaml)
        return self.credentials_dict

    def init_engine(self, credentials_read):
        """This takes the instance from read_creds(),
          initialise and return an sqlalchemy database engine
        """
        self.credentials = credentials_read['credentials']
        DATABASE_TYPE = self.credentials['database_type']
        DBAPI = self.credentials['dbapi']
        USER = self.credentials['user']
        PASSWORD = self.credentials['password']
        HOST = self.credentials['host']
        PORT = self.credentials['port']
        DATABASE = self.credentials['database']

        connection_string = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?charset=utf8mb4"

        engine1 = sqlalchemy.create_engine(connection_string)
        return engine1

    def data_sample(self, engine, table_name: str, row_number: int):
        """This queries the database,
        extracts the xth rows of data from the table
        (pinterest_data, geolocation_data, or user_data)
        and returns the data in dictionary format
        """
        data_query = text(f"SELECT * FROM {table_name} LIMIT {row_number}, 1")
        with engine.connect() as connection:
            result = connection.execute(data_query)
        return dict(next(result)._mapping)


def datetime_converter(date_time):
    """ This converts datetime to string """
    if isinstance(date_time, datetime.datetime):
        return date_time.__str__()


def data_emulation(engine):
    """This connects to the database and emulate data to Kafka topics using API Invoke URL.
    The data is sent from three tables to their corresponding Kafka topic:
    - pin: for the Pinterest posts data
    - geo: for the post geolocation data
    - user: for the post user data

    The argument is the instance from read_creds and init_engine:
    credentials_read = new_connector.read_creds('db_creds.yaml')
    this_engine = new_connector.init_engine(credentials_read)
    """
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

        with engine.connect() as connection:
            tablenames_topics = {'pinterest_data': '0a5e6ec37a2f.pin',
                                 'geolocation_data': '0a5e6ec37a2f.geo',
                                 'user_data': '0a5e6ec37a2f.user'}
            for table_name, topic in tablenames_topics.items():
                query_string = sqlalchemy.text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
                selected_row = connection.execute(query_string)
                invoke_url=f"https://bj0k1iog5m.execute-api.us-east-1.amazonaws.com/production/{topic}"
                for row in selected_row:
                    result = dict(row._mapping)
                    payload = json.dumps({"records": [{"value": result}]},
                                         default=datetime_converter)
                    response = requests.request("POST", invoke_url,
                                                headers=headers, data=payload)


new_connector = AWSDBConnector()

if __name__ == "__main__":
    credentials_read = new_connector.read_creds('db_creds.yaml')
    this_engine = new_connector.init_engine(credentials_read)

    sample_pinterest_data = new_connector.data_sample(this_engine, 
                                                      "pinterest_data", 5)
    print(f"This is pinterest data {sample_pinterest_data}")

    sample_geolocation_data = new_connector.data_sample(this_engine, 
                                                        "geolocation_data", 5)
    print(f"This is geolocation data {sample_geolocation_data}")

    sample_user_data = new_connector.data_sample(this_engine, "user_data", 5)
    print(f"This is user data {sample_user_data}")

    data_emulation(this_engine)
