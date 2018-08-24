import yaml
import pytest
import pandas as pd
from facilities_dataloader.helper import read_data, create_mysql_engine, data_to_db

def test_setup():
    engine = create_mysql_engine('./tests/test_creds.yml')
    engine.execute('CREATE DATABASE IF NOT EXISTS testdb;')
    engine.execute('USE testdb')
    engine.execute('DROP TABLE IF EXISTS elec_usage;')
    engine.execute('''CREATE TABLE IF NOT EXISTS elec_usage (
                        invoice_id VARCHAR(16) PRIMARY KEY,
                        statement_number VARCHAR(10),
                        account_number VARCHAR(10),
                        bill_month DATE,
                        acctg_month DATE,
                        service_period_start DATE,
                        service_period_stop DATE,
                        rebill VARCHAR(1),
                        billed_khw FLOAT,
                        peak_kw FLOAT,
                        supply_charges FLOAT,
                        udc_charges FLOAT,
                        total_charges FLOAT
                        );''')

def test_read_electricity_data_from_xlsx():
    assert type(read_data('/home/vidal/Projects/cityofchicago/2FM/data/energy/energy.xlsx', 'elec')) == pd.core.frame.DataFrame

def test_connection_made_via_mysql_engine():
    engine = create_mysql_engine('./tests/test_creds.yml')
    assert type(engine.table_names()) is list, "Connection to database failed."

def test_data_sends_to_database():
    engine = create_mysql_engine('./tests/test_creds.yml')
    df = pd.DataFrame([['a','b'],[1,2]], columns=['first', 'second'])
    data_to_db(df, 'sample', engine)
    result = pd.read_sql(sql='SELECT * FROM sample;', con=engine)
    assert list(result.columns) == ['first','second']
