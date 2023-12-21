import datetime
import json
import requests
import random
import sqlalchemy
from sqlalchemy import text
from time import sleep
import yaml

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


def datetime_converter(date_time):
    """ This converts datetime to string """
    if isinstance(date_time, datetime.datetime):
        return date_time.__str__()


def data_emulation_streaming(engine):
    """
    This connects to the database and emulate data to kafka topics
    using API Invoke URL.
    The streaming data is sent from three tables to their corresponding topic:
    - pin: for the Pinterest posts data
    - geo: for the post geolocation data
    - user: for the post user data

    The argument is the instance from read_creds and init_engine:
    credentials_read = new_connector.read_creds('db_creds.yaml')
    this_engine = new_connector.init_engine(credentials_read)
    """
    headers = {'Content-Type': 'application/json'}
    with engine.connect() as connection:

        while True:
            """
            This creates connection to the database to emulate randomly
            generated data into Kinesis Streams
            """
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)
            headers = {'Content-Type': 'application/json'}

            with engine.connect() as connection:
                tablenames_streams = {'pinterest_data':
                                      "streaming-0a5e6ec37a2f-pin",
                                      'geolocation_data':
                                      "streaming-0a5e6ec37a2f-geo",
                                      'user_data':
                                      "streaming-0a5e6ec37a2f-user"}
                for table_name, stream_name in tablenames_streams.items():
                    sql_string = sqlalchemy.text(f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
                    selected_row = connection.execute(sql_string)
                    invoke_url =\
                        f"https://bj0k1iog5m.execute-api.us-east-1.amazonaws.com/production/streams/{stream_name}/record"
                    for row in selected_row:
                        result = dict(row._mapping)
                        payload = json.dumps({"StreamName": f"{stream_name}",
                                              "Data": result,
                                              "PartitionKey": "test"},
                                             default=datetime_converter)
                        response = requests.request("PUT", 
                                                    invoke_url,
                                                    headers=headers,
                                                    data=payload)
                        print(response.status_code)


new_connector = AWSDBConnector()

if __name__ == "__main__":
    credentials_read = new_connector.read_creds('db_creds.yaml')
    this_engine = new_connector.init_engine(credentials_read)

    data_emulation_streaming(this_engine)
