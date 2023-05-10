'''
Unit tests for the functions included in 
the "data_collector.py" component

Author: Vitor Abdo
Date: May/2023
'''

# import necessary packages
import datetime
import pytest

from components.data_collector import collect_from_kaggle
from components.data_collector import read_raw_csv_data
from components.data_collector import read_raw_json_data

@pytest.mark.parametrize(
        'username, page_name, file_name, path_to_save',
        [
            ('chickooo', 'top-tech-startups-hiring-2023', 'json_data.json', './data'),
            ('loganlauton', 'nba-players-and-team-data', 'NBA Payroll(1990-2023).csv', './data'),
            ('loganlauton', 'nba-players-and-team-data', 'NBA Player Box Score Stats(1950 - 2022).csv', './data'),
            ('loganlauton', 'nba-players-and-team-data', 'NBA Player Stats(1950 - 2022).csv', './data'),
            ('loganlauton', 'nba-players-and-team-data', 'NBA Salaries(1990-2023).csv', './data')
        ]
)
def test_collect_from_kaggle():
    '''Test for Kaggle API with "collect_from_kaggle.py"
      function in the "data_collector.py" file'''


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
