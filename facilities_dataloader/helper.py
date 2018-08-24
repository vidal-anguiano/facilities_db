import os
import math
import yaml
import sqlalchemy
import pandas as pd
from mysql.connector.errors import IntegrityError

def read_data(filepath, dataset, sep=','):
    '''
    Reads data from excel file into a DataFrame and fixes data column types.

    Parameters
    ----------
    filepath: (string) Filepath for the excel document containing the desired data.

    Returns
    -------
    dataset: (string) {elec, gas} String indicating which dataset is being read.
    '''
    # Used to change the data type of the columns specified below
    col_types = {'elec': {'STATEMENTNO': str},
                 'gas_accounts':  {'install_date': pd.to_datetime,
                                    'ert_install_date': pd.to_datetime},
                 'buildings': {'zipcode': str,
                               'nrel_bca_leed_analysis': str,
                               'nrel_renewable_reopt_analysis': str,
                               'retrocommision': str,
                               'energy_assessment': str,
                               'gr_sq_ft_location2': str},
                 'other': {'account_no': str}}
    assert type(filepath) == str, "You must provide the filepath as a string."
    assert os.path.exists(filepath) == 1, "File does not exist."
    if '.csv' in filepath:
        data = pd.read_csv(filepath, converters=col_types[dataset], sep=sep)
    else:
        data = pd.read_excel(filepath, converters=col_types[dataset])

    return data


def create_mysql_engine(creds_path):
    '''
    Create engine to connect to a database.

    Parameters
    ----------
    creds_path: (string) Path to YAML file containing database credentials.

    Returns
    -------
    engine: (sqlalchemy.engine.base.Engine) Engine used to establish connection
    '''
    creds = yaml.load(open(creds_path))
    user, password, host, database = creds['user'], creds['pass'], creds['host'], creds['database']
    engine = sqlalchemy.create_engine('mysql+mysqlconnector://{}:{}@{}/{}'.format(user, password, host, database))
    return engine

def data_to_db(data, tablename, engine, if_exists='append'):
    '''
    Append usage data to the appropriate database table.

    Parameters
    ----------
    data: (DataFrame) DataFrame containing new usage data to append.
    tablename: (string) Name of the table to append data to.
    engine: (sqlalchemy.engine.base.Engine) Connection to database.
    if_exists: (string) If the table already exists, the new data will be appended to the existing table.

    Returns
    -------
    message: (string) String verifying that there are more rows in table.
    '''
    assert type(engine) == sqlalchemy.engine.base.Engine, "Make sure to provide engine."
    assert type(data) == pd.core.frame.DataFrame, "Input a DataFrame, not a {}".format(type(data))
    assert type(tablename) == str, "Tablename must be a string, not a {}.".format(type(tablename))
    try:
        data.to_sql(tablename, engine, if_exists='append', index=False)
    except Exception as e:
        print(str(e).split('[SQL')[0])
    return None


def restore_leading_zeros(number, n):
    '''
    Fix numbers that lost its leading zeros until number has n digits.

    Parameters
    ----------
    number: (string or int) number that lost leading zeros
    n: (int) number of digits final number should have

    Returns
    -------
    number: (string) number with n digits and leading zeros restored
    '''
    if type(number) == float:
        return math.nan
    else:
        number = str(number)
        while len(number) < n:
            number = '0' + number

    return number
