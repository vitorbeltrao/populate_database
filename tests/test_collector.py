'''
Unit tests for the functions included in
the "data_collector.py" component

Author: Vitor Abdo
Date: May/2023
'''

# import necessary packages
import os
import pytest

from components.data_collector import collect_from_kaggle
from components.data_collector import read_raw_csv_data
from components.data_collector import read_raw_json_data


@pytest.mark.parametrize(
    'username, page_name, file_name, path_to_save',
    [
        ('chickooo', 'top-tech-startups-hiring-2023', 'json_data.json', 'populate_database/data'),
        ('loganlauton', 'nba-players-and-team-data', 'NBA Payroll(1990-2023).csv', 'populate_database/data'),
        ('loganlauton', 'nba-players-and-team-data', 'NBA Player Box Score Stats(1950 - 2022).csv', 'populate_database/data'),
        ('loganlauton', 'nba-players-and-team-data', 'NBA Player Stats(1950 - 2022).csv', 'populate_database/data'),
        ('loganlauton', 'nba-players-and-team-data', 'NBA Salaries(1990-2023).csv', 'populate_database/data')
    ]
)


def test_collect_from_kaggle(
        username, page_name, file_name, path_to_save, temp_dir):
    '''Test the collect_from_kaggle function. This test
    verifies that the collect_from_kaggle function
    correctly downloads a dataset from Kaggle, extracts
    the ZIP file, and saves it to the specified path
    '''
    temp_path_to_save = os.path.join(temp_dir, path_to_save)
    expected = os.path.join(temp_path_to_save, file_name)

    collect_from_kaggle(username, page_name, file_name, temp_path_to_save)
    assert os.path.exists(expected)


def test_import_raw_csv_data(raw_csv_data_path):
    '''tests the "read_raw_csv_data" function
    made in the "data_collector.py" file
    '''
    raw_df = read_raw_csv_data(raw_csv_data_path)
    assert raw_df.shape[0] > 0 and raw_df.shape[1] > 0


def test_import_raw_json_data(raw_json_data_path):
    '''tests the "read_raw_json_data" function
    made in the "data_collector.py" file
    '''
    raw_df = read_raw_json_data(raw_json_data_path)
    assert raw_df.shape[0] > 0 and raw_df.shape[1] > 0
