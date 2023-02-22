from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

class DB:
    def __init__(self, conn_string):
        self.engine = None
        self._connect(conn_string)

    def _connect(self, conn_string):
        engine = None
        try:
            print('Connecting to the MySQL...........')
            engine = create_engine(conn_string)
            print("Connection successfully..................")
        except SQLAlchemyError as err:
            print("Error while connecting to MySQL", err)
            engine = None
        self.engine = engine

    def insert_many(self, table_name, df):
        try:
            print("Inserting data into %s ..." % table_name)
            df.to_sql(table_name, con=self.engine, index=False, if_exists='append',chunksize=1000)
            print("Data inserted into %s successfully" % table_name)
        except SQLAlchemyError as err:
            print("Error while inserting to MySQL", str(err.__dic__['orig']))

def read_file(filepath, filename, extension):
    if extension.lower() == 'json':
        print(filepath+filename+'.'+extension.lower())
        return pd.read_json(filepath+filename+'.'+extension.lower())
    elif extension.lower() == 'csv':
        return pd.read_csv(filepath+filename+'.'+extension.lower(), index_col=False)
    else:
        raise Exception("File extension : %s is not supported"%extension)
    