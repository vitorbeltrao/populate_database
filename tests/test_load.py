'''
Unit tests for the functions included in
the "data_load.py" component

Author: Vitor Abdo
Date: May/2023
'''

# import necessary packages
from components.data_load import create_schema_into_postgresql
from components.data_load import create_table_into_postgresql
from components.data_load import insert_data_into_postgresql


def test_create_schema_into_postgresql(mocker):
    '''tests the "create_schema_into_postgresql" function
    made in the "data_load.py" file
    '''
    # Create a mock cursor and patch the psycopg2 connect method to return the mock cursor
    mock_cursor = mocker.Mock()
    mocker.patch("psycopg2.connect").return_value.cursor.return_value = mock_cursor
    
    # Call the function being tested with test arguments
    create_schema_into_postgresql(
        host_name="localhost",
        db_name="test_db",
        user_name="test_user",
        password="test_password",
        schema_name="test_schema"
    )
    
    # Check that the expected SQL query was executed
    mock_cursor.execute.assert_called_once_with(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'test_schema'"
    )
