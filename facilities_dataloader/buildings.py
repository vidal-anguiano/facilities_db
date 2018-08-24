import math
from facilities_dataloader.helper import read_data, create_mysql_engine, data_to_db

CREDS = 'creds.yml'

def buildings_data_to_db(filepath):
    assert type(filepath) == str, 'Please provide a file path as a string.'
    engine = create_mysql_engine(CREDS)
    buildings = read_data(filepath, 'buildings')
    buildings = buildings.replace('NV', math.nan)
    data_to_db(buildings, 'buildings', engine)
