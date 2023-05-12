'''
This .py file is for creating the fixtures

Author: Vitor Abdo
Date: May/2023
'''

# import necessary packages
import pytest
import tempfile
import pandas as pd
from decouple import config

# config
OPEN_POSITIONS_RAW_PATH = config('OPEN_POSITIONS_RAW_PATH')
NBA_PAYROLL_RAW_PATH = config('NBA_PAYROLL_RAW_PATH')
NBA_PLAYER_BOX_RAW_PATH = config('NBA_PLAYER_BOX_RAW_PATH')


@pytest.fixture
def temp_dir():
    '''Fixture that creates a temporary directory for use in tests.
    This fixture uses Python's built-in `tempfile.TemporaryDirectory()`
    to create a temporary directory that can be used during testing.
    The directory is automatically deleted when the fixture goes out
    of scope

    Yields:
        str: The path to the temporary directory.

    Examples:
        The fixture can be used in a test function like this:

        ```
        def test_my_function(temp_dir):
            # Do something with the temporary directory
            my_function(temp_dir)
        ```
    '''
    with tempfile.TemporaryDirectory() as temp:
        yield temp


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


@pytest.fixture(scope='session')
def raw_json_df():
    '''Fixture to generate raw json data path to our tests'''
    data_path = OPEN_POSITIONS_RAW_PATH

    if data_path is None:
        pytest.fail('You must provide the json file')

    raw_json_df = pd.read_json(data_path)

    return raw_json_df


@pytest.fixture(scope='session')
def raw_csv_df():
    '''Fixture to generate raw csv data path to our tests'''
    data_path = NBA_PAYROLL_RAW_PATH

    if data_path is None:
        pytest.fail('You must provide the csv file')

    raw_csv_df = pd.read_csv(data_path)

    return raw_csv_df


@pytest.fixture(scope='session')
def raw_csv_df_datetime():
    '''Fixture to generate raw csv data path to our tests'''
    data_path = NBA_PLAYER_BOX_RAW_PATH

    if data_path is None:
        pytest.fail('You must provide the csv file')

    raw_csv_df = pd.read_csv(data_path)

    return raw_csv_df
