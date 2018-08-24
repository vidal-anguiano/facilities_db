import pytest
import pandas as pd
from facilities_dataloader.helper import read_data, create_mysql_engine, data_to_db
from facilities_dataloader.natural_gas import preprocess_natural_gas, natural_gas_data_to_db, fix_new_account_nmbr

def test_account_number_split_by_hyphen():
    result = fix_new_account_nmbr('532322315-24')
    assert '-' in result, "The account number provided has no hyphen."

def test_account_number_fixed():
    result = fix_new_account_nmbr('522322315-2')
    assert result == '0522322315-00002', "The account number was not fixed."

def test_account_number_fixed2():
    result = fix_new_account_nmbr('522322315-244')
    assert result == '0522322315-00244', "The account number was not fixed."

def test_account_numberfixed3():
    result = fix_new_account_nmbr(0.0)
    assert str(result) == 'nan'
