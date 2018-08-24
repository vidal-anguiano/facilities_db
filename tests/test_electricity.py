import pytest
import pandas as pd
from facilities_dataloader.helper import read_data, create_mysql_engine, data_to_db
from facilities_dataloader.electricity import preprocess_electricity, electricity_data_to_db

def test_no_duplicate_invoice_ids_elec():
    result = preprocess_electricity('/home/vidal/Projects/cityofchicago/2FM/data/energy/energy.xlsx')
    assert len(result['invoice_id'].unique()) == len(result), "There are duplicate invoice numbers in this data."

def test_elec_column_names_renamed_appropriately():
    result = preprocess_electricity('/home/vidal/Projects/cityofchicago/2FM/data/energy/energy.xlsx')
    assert 'statement_number' in result.columns

def test_statement_no_column_formatted_as_str():
    result = preprocess_electricity('/home/vidal/Projects/cityofchicago/2FM/data/energy/energy.xlsx')
    assert result.statement_number.dtype == object

def test_elec_no_of_columns_correct():
    result = preprocess_electricity('/home/vidal/Projects/cityofchicago/2FM/data/energy/energy.xlsx')
    assert result.shape[1] == 13

def test_send_elec_data_to_database():
    engine = create_mysql_engine('./tests/test_creds.yml')
    data = preprocess_electricity('/home/vidal/Projects/cityofchicago/2FM/data/energy/energy.xlsx')
    data_to_db(data, 'elec_usage', engine)
