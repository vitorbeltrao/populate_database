'''
This .py file is for creating the fixtures

Author: Vitor Abdo
Date: May/2023
'''

# import necessary packages
import pytest
from decouple import config

# config
OPEN_POSITIONS_RAW_PATH = config('OPEN_POSITIONS_RAW_PATH')
NBA_PAYROLL_RAW_PATH = config('NBA_PAYROLL_RAW_PATH')


@pytest.fixture(scope='session')
def raw_csv_data_path():
    '''Fixture to generate raw csv data path to our tests'''
    data_path = NBA_PAYROLL_RAW_PATH

    if data_path is None:
        pytest.fail('You must provide the csv file')

    return data_path


@pytest.fixture(scope='session')
def raw_json_data_path():
    '''Fixture to generate raw json data path to our tests'''
    data_path = OPEN_POSITIONS_RAW_PATH

    if data_path is None:
        pytest.fail('You must provide the csv file')

    return data_path
