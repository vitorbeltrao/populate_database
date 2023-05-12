'''
Unit tests for the functions included in
the "data_transform.py" component

Author: Vitor Abdo
Date: May/2023
'''

# import necessary packages
import pandas as pd
from pandas.testing import assert_frame_equal

from components.data_transform import transform_json_data
from components.data_transform import transform_string_to_float
from components.data_transform import transform_string_to_datetime
from components.data_transform import create_auxiliary_columns


def test_transform_json_data(raw_json_df):
    '''tests the "transform_json_data" function
    made in the "data_transform.py" file
    '''
    # building the exptected output
    columns_to_drop = ['id', 'logo_url']
    columns_to_convert_to_str = ['tags', 'locations', 'industries']

    expected_output = raw_json_df.copy()
    expected_output.drop(columns_to_drop, axis=1, inplace=True)
    for col in columns_to_convert_to_str:
        expected_output[col] = expected_output[col].apply(lambda x: ','.join(map(str, x)))
    expected_output2 = pd.json_normalize(expected_output['jobs'])
    expected_output2.fillna(0, inplace=True)

    final_expected_output = pd.concat([expected_output, expected_output2], axis=1).drop('jobs', axis=1)

    # building the actual output using the function that we want to test
    actual_output = transform_json_data(raw_json_df, columns_to_drop, columns_to_convert_to_str, 'jobs')

    assert_frame_equal(actual_output, final_expected_output)


def test_transform_string_to_float(raw_csv_df):
    '''tests the "transform_string_to_float" function
    made in the "data_transform.py" file
    '''
    columns_to_convert_to_float = ['payroll']
    transformed_df = transform_string_to_float(
        raw_csv_df, columns_to_convert_to_float)
    
    assert transformed_df['payroll'].dtypes == float


def test_transform_string_to_datetime(raw_csv_df_datetime):
    '''tests the "transform_string_to_datetime" function
    made in the "data_transform.py" file
    '''
    transformed_df = transform_string_to_datetime(
        raw_csv_df_datetime, 'GAME_DATE')
    
    assert transformed_df['GAME_DATE'].dtypes == 'datetime64[ns]'


def test_create_auxiliary_columns(raw_csv_df):
    '''tests the "create_auxiliary_columns" function
    made in the "data_transform.py" file
    '''
    create_auxiliary_columns(raw_csv_df)

    assert all([item in raw_csv_df.columns for item in ['id','created_at', 'updated_at']])
