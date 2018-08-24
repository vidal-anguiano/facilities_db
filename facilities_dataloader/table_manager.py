import yaml
from os import listdir
from os.path import join
from facilities_dataloader.helper import create_mysql_engine

CONFIG = yaml.load(open('creds.yml'))

def process_sql_statements(filepath):
    '''
    Splits multiple statements in a .sql file and returns the statements in a list.

    Parameters
    ----------
    filepath: (string) filepath for .sql file

    Returns
    -------
    statements: (list of strings) list of string SQL statements
    '''
    statements = open(filepath).read().replace('\n',' ').split(';')
    l = len(statements)
    return statements[:l-1]

def execute_sql_from_files(credentials, files):
    engine = create_mysql_engine(credentials)
    for file in files:
        statements = process_sql_statements(join(CONFIG['ddl_directory'], file))
        for statement in statements:
            engine.execute(statement)

def create_tables(credentials):
    create_files = [f for f in listdir(CONFIG['ddl_directory']) if 'create' in f]
    execute_sql_from_files(credentials, create_files)

def drop_tables(credentials):
    drop_files = [f for f in listdir(CONFIG['ddl_directory']) if 'drop' in f]
    execute_sql_from_files(credentials, drop_files)
