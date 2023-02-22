import util
from util import DB
import yaml
from yaml.loader import SafeLoader
from multiprocessing import Pool
from timebudget import timebudget

def read_config(config_file_location):
    config = None
    with open(config_file_location) as f:
        config = yaml.load(f, Loader=SafeLoader)
    return config


def etl(data_config, db_config):

    # Extract Data
    data = util.read_file(
    data_config['directory'], data_config['filename'], data_config['extension'])
    # Connect to Destination
    conn_string = "mysql+pymysql://%s:%s@%s:%s/%s" % (
    db_config['username'], db_config['password'], db_config['host'], db_config['port'], db_config['dbname'])
    db = DB(conn_string)
    # Load to destination
    db.insert_many(db_config['destination_table_name'], data)
    return True

@timebudget
def run(operation, input, pool):
    return pool.starmap_async(operation, input)


if __name__ == '__main__':
    # Read Config
    configs = read_config('./config.yaml')
    process_pool = Pool(4)
    for config in configs:
        run(etl, [(config['data'], config['db'])], process_pool)
    process_pool.close()
    process_pool.join()
