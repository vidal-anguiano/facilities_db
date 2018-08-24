'''

'''
import sys
import os
import argparse
import sqlalchemy
from facilities_dataloader.helper import read_data, create_mysql_engine, data_to_db
from facilities_dataloader.table_manager import create_tables, drop_tables
from facilities_dataloader.buildings import buildings_data_to_db
from facilities_dataloader.electricity import preprocess_electricity, electricity_data_to_db, elec_accounts_to_db
from facilities_dataloader.natural_gas import preprocess_natural_gas, natural_gas_data_to_db, ngas_accounts_to_db
# from facilities_dataloader.helper import

CREDS = 'creds.yml'

def driver(args):
    if args.create_tables:
        create_tables(CREDS)

    if args.drop_tables:
        response = input('''WARNING: You are about to delete and recreate the buildings, elec_usage, and ngas_usage tables, are you sure you want to continue? [y/n]''')
        if response in ['Yes','y','Y']:
            drop_tables(CREDS)
        else:
            print('Process killed.')

    if args.load_buildings:
        buildings_data_to_db(args.load_buildings)

    if args.load_elec:
        electricity_data_to_db(args.load_elec)

    if args.load_ngas:
        natural_gas_data_to_db(args.load_ngas)

    if args.load_elec_accounts:
        elec_accounts_to_db(args.load_elec_accounts)

    if args.load_ngas_accounts:
        ngas_accounts_to_db(args.load_ngas_accounts)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='''Application to upload
                                     facilities operations facility and energy
                                     use data.''')
    parser.add_argument('--create_tables', help='', action='store_true')
    parser.add_argument('--drop_tables', help='', action='store_true')
    parser.add_argument('--load_buildings', help='') # implemented
    parser.add_argument('--load_elec', help='')       # implemented
    parser.add_argument('--load_ngas', help='') # implemented
    parser.add_argument('--load_elec_accounts', help='')
    parser.add_argument('--load_ngas_accounts', help='')

    args = parser.parse_args()
    driver(args)
