import math
from facilities_dataloader.helper import read_data, create_mysql_engine, data_to_db, restore_leading_zeros

CREDS = 'creds.yml'


def fix_invoice_id(invoice_id):
    '''
    If invoice_ids that are missing leading zeros, this function will fill in
    the dropped zeros and fix the invoice ids.

    Parameters
    ----------
    invoice_id: (string) invoice_id without leading zeros

    Returns
    -------
    fixed_invoice_id: (string) reformatted invoice id with restored zeros
    '''
    if type(invoice_id) == float:
        return math.nan
    else:
        assert '-' in invoice_id, "NInvoice ids must have a '-' character\
        in order to be valid. Make sure each one as a hyphen."
        split = invoice_id.split('-')
        while len(split[0]) < 10:
            split[0] = '0' + split[0]
        if split[1][-1] in ['R','C','Z']:
            while len(split[1]) < 5:
                split[1] = '0' + split[1]
        else:
            while len(split[1]) < 4:
                split[1] = '0' + split[1]

    assert len(split[0]) == 10, "Something went wrong with fixing invoice id, it's of length {}, and it's {}.".format(len(split[0]), split[0])
    assert len(split[1]) <= 5, "Something went wrong with fixing invoice id, it's of length {}, and it's {}.".format(len(split[1]), split[1])

    fixed_invoice_id = '-'.join(split)

    return fixed_invoice_id


def preprocess_electricity(filepath):
    '''
    Load, clean, and prepare electricity data for loading into MySQL database.

    Parameters
    ----------
    filepath: (string) location of electricity file.

    Returns
    -------
    data: (DataFrame) cleaned and preprocessed dataframe.
    '''
    data = read_data(filepath, 'elec')
    col_names = {'Funds': 'funds',
                     'NonConsec?': 'nonconsec',
                     'Discard?': 'discard',
                     'Num': 'num',
                     'ACCOUNTID': 'account_id',
                     'STATEMENTNO': 'statement_number',
                     'UDCACCTID': 'account_number',
                     'INVOICEID': 'invoice_id',
                     'INVOICEDATE': 'invoice_date',
                     'SERVICE_PERIOD_START': 'service_period_start',
                     'SERVICE_PERIOD_STOP': 'service_period_stop',
                     'BILLEDKWH': 'billed_khw',
                     'Peak kW': 'peak_kw',
                     'SUPPLY CHARGES': 'supply_charges',
                     'UDC CHARGES': 'udc_charges',
                     'Acctnum': 'acctnum',
                     'Cancel / Rebill?': 'rebill',
                     'STATENUM': 'statenum',
                     'BILL MO': 'bill_month',
                     'ACCTG MO': 'acctg_month'}

    # Ensuring that the electricity data are being loaded.
    assert list(data.columns) == list(col_names.keys()), "Make sure column names match the columns as provided in the documentation."

    # Removing records flagged for removal.
    data = data[~data['Discard?'].isin(['Y'])]

    # Renaming columns using names above.
    data = data.rename(columns = col_names)

    # Fix account number to restore dropped leading zeros and ensure it has 10 digits
    data['account_number'] = data['account_number'].apply(lambda x: restore_leading_zeros(x, 10))

    # Fix invoice id to ensure it has 10-4 digits by restoring dropped zeros
    data['invoice_id'] = data['invoice_id'].apply(fix_invoice_id)

    # Replacing instances of "Multiple Demands" with "NULL"
    data['peak_kw'] = data['peak_kw'].apply(lambda x: math.nan if x == 'Multiple Demands' else x)

    col_order = ['invoice_id',
                 'statement_number',
                 'account_number',
                 'bill_month',
                 'acctg_month',
                 'service_period_start',
                 'service_period_stop',
                 'rebill',
                 'billed_khw',
                 'peak_kw',
                 'supply_charges',
                 'udc_charges']

    data = data[col_order]
    data['total_charges'] = data['supply_charges'] + data['udc_charges']

    return data


def electricity_data_to_db(filepath):
    '''
    Sends electricity data to a database table named elec_usage.

    Parameters
    ----------
    filepath: (string) filepath for file containing electricity utility data.

    Returns
    -------
    None
    '''
    assert type(filepath) == str, 'Please provide a file path as a string.'
    engine = create_mysql_engine(CREDS)
    elec_data = preprocess_electricity(filepath)
    data_to_db(elec_data, 'elec_usage', engine)


def elec_accounts_to_db(filepath):
    '''
    Sends electricity accounts data to database table named elec_accounts.

    Parameters
    ----------
    filepath: (string) filepath for file containing electricity accounts data.

    Returns
    -------
    None
    '''
    assert type(filepath) == str, 'Please provide a file path as a string.'
    engine = create_mysql_engine(CREDS)
    elec_accounts = read_data(filepath, 'other')
    elec_accounts = elec_accounts.drop_duplicates()
    elec_accounts['account_number'] = elec_accounts['account_number'].apply(lambda x: restore_leading_zeros(x, 10))
    data_to_db(elec_accounts, 'elec_accounts', engine)
