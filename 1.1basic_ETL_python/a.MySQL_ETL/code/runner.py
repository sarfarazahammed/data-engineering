import pandas as pd
import util
from util import DB
import yaml
from yaml.loader import SafeLoader

def read_config(config_file_location):
    config = None
    with open('./config.yaml') as f:
        config = yaml.load(f, Loader=SafeLoader)
    return config


# Read Config
config = read_config('config.yaml')
data_config = config['data']
db_config = config['db']
# Extract Data
data = util.read_file(data_config['directory'], data_config['filename'], data_config['extension'])


# Connect to Destination

conn_string = "mysql+pymysql://%s:%s@%s:%s/%s" % (
    db_config['username'], db_config['password'], db_config['host'], db_config['port'], db_config['dbname'])

db = DB(conn_string)

# Load to destination
db.insert_many(db_config['destination_table_name'], data)
