import pytest
import yaml
from facilities_dataloader.helper import create_mysql_engine
from facilities_dataloader.table_manager import create_tables, drop_tables

CREDS = 'tests/test_creds.yml'
DB = yaml.load(open(CREDS))['database']

def fetch_tables():
    engine = create_mysql_engine(CREDS)
    result = engine.execute("SELECT table_name FROM information_schema.tables where table_schema = '{}'".format(DB)).fetchall()
    tables = [row[0] for row in result]
    return tables

def test_tables_created_elec_usage():
    create_tables(CREDS)
    tables = fetch_tables()
    assert 'elec_usage' in tables, "Tables were not successfully created."

def test_tables_dropped_elec_usage():
    drop_tables(CREDS)
    tables = fetch_tables()
    assert 'elec_usage' not in tables, "Tables were not successfully dropped."

def test_tables_created_gas_usage():
    create_tables(CREDS)
    tables = fetch_tables()
    assert 'ngas_usage' in tables, "Tables were not successfully created."

def test_tables_dropped_gas_usage():
    drop_tables(CREDS)
    tables = fetch_tables()
    assert 'ngas_usage' not in tables, "Tables were not successfully dropped."
