import math
import datetime
import pandas as pd
from facilities_dataloader.helper import read_data, create_mysql_engine, data_to_db, restore_leading_zeros

CREDS = 'creds.yml'

def fix_new_account_nmbr(acct_number):
    '''
    If new account numbers are missing leading zeros, this function will replace
    the zeros and fix the account numbers.

    Parameters
    ----------
    acct_number: (string) new account number without leading zeros

    Returns
    -------
    fixed_account_number: (string) reformatted account number with replaced zeros
    '''
    if type(acct_number) == float:
        return math.nan
    else:
        assert '-' in acct_number, "New account numbers must have a '-' character\
        in order to be valid. Make sure each one as a hyphen."
        split = acct_number.split('-')
        while len(split[0]) < 10:
            split[0] = '0' + split[0]
        while len(split[1]) < 5:
            split[1] = '0' + split[1]

    assert len(split[0]) == 10, "Something went wrong with fixing account number."
    assert len(split[1]) == 5, "Something went wrong with fixing account number."

    return '-'.join(split)


def current_account_number(data):
        account_number = data['new_account_number'] if data['new_account_number'] is not math.nan else data['account_number']
        return account_number


def preprocess_natural_gas(filepath):
    '''
    Load, clean, and prepare natural_gas data for loading into MySQL database.

    Parameters
    ----------
    filepath: (string) location of natural_gas file.

    Returns
    -------
    data: (DataFrame) cleaned and preprocessed dataframe.
    '''
    data = read_data(filepath, 'other')
    col_names = {"ADDRESS": "address",
                 "Address 2": "address2",
                 "City": "city",
                 "Account Number": "account_number",
                 "New Account Number": "new_account_number",
                 "Start Date": "service_period_start",
                 "End Date": "service_period_stop",
                 "Therms": "therms",
                 "Utility Amount": "utility_amount",
                 "Supplier Amount": "supplier_amount"}

    # Ensuring that the natural_gas data are being loaded.
    data = data[list(col_names.keys())]
    assert list(data.columns) == list(col_names.keys()), "Make sure column names match the columns as provided in the documentation."

    # Renaming columns using names above
    data = data.rename(columns = col_names)

    # Replacing instances of unaccepted characters with "NULL"
    data = data.replace(["-", "N/A", "#N/A"], math.nan)

    # Creates a column with the first day of the month using the period end date.
    data['bill_month'] = data['service_period_stop'].apply(lambda x: x.replace(day=1))

    # Creating total_amount column. Note: NaN/Null value in either results in NaN/Null sum.
    data['total_amount'] = data['utility_amount'] + data['supplier_amount']

    # Restores leading zeros in account number so that it has 13 digits
    data['account_number'] = data['account_number'].apply(lambda x: restore_leading_zeros(x, 13))

    # Reformats the account number to ensure leading zeros are replaced where lost.
    data['new_account_number'] = data['new_account_number'].apply(fix_new_account_nmbr)

    # Takes new account number where exists and uses account nubmer otherwise.
    data['current_account_number'] = data.apply(current_account_number, axis=1)

    data = data.drop_duplicates()

    col_order = ['account_number',
                 'new_account_number',
                 'current_account_number',
                 'bill_month',
                 'service_period_start',
                 'service_period_stop',
                 'therms',
                 'utility_amount',
                 'supplier_amount',
                 'total_amount',
                 'address',
                 'address2',
                 'city']

    data = data[col_order]

    return data


def natural_gas_data_to_db(filepath):
    '''
    Sends natural_gas data to a database table named elec_usage.

    Parameters
    ----------
    filepath: (string) filepath for file containing natural_gas utility data.

    Returns
    -------
    None
    '''
    assert type(filepath) == str, 'Please provide a file path as a string.'
    engine = create_mysql_engine(CREDS)
    ngas_data = preprocess_natural_gas(filepath)
    data_to_db(ngas_data, 'ngas_usage', engine)


def ngas_accounts_to_db(filepath):
        '''
        Sends natural gas accounts data to database table named ngas_accounts.

        Parameters
        ----------
        filepath: (string) filepath for file containing natural gas account data.

        Returns
        -------
        None
        '''
        assert type(filepath) == str, 'Please provide a file path as a string.'
        engine = create_mysql_engine(CREDS)
        ngas_accounts = read_data(filepath, 'gas_accounts')
        ngas_accounts = ngas_accounts.drop_duplicates()
        ngas_accounts['account_number'] = ngas_accounts['account_number'].apply(lambda x: restore_leading_zeros(x, 13))
        ngas_accounts['ert_number'] = ngas_accounts['ert_number'].apply(lambda x: restore_leading_zeros(x, 9))
        data_to_db(ngas_accounts, 'ngas_accounts', engine)
