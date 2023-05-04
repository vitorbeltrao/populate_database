'''
Script to perform some basic data transformations
to feed the database, without mess

Author: Vitor Abdo
Date: April/2023
'''

# import necessary packages
import logging
import pandas as pd
import datetime as dt

logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s')


def transform_json_data(
        raw_df: pd.DataFrame,
        columns_to_drop: list,
        list_of_columns: list,
        columns_to_json_normalize: str) -> pd.DataFrame:
    '''Make the necessary transformations on the dataframe that is in json format
    The transformations are:
    dropping unnecessary columns;
    remove entries from columns that are as list to string;
    remove entries from columns that are in the dictionary
    so that they compose the dataframe correctly.

    :param raw_df: (dataframe)
    Pandas dataframe that we want to perform the transformations

    :param columns_to_drop: (list)
    List columns we want to delete from the dataframe

    :param list_of_columns: (list)
    List of columns we want to convert from list to string

    :param columns_to_json_normalize: (str)
    Column that we want to normalize so as not to leave it in dictionary format

    :return: (dataframe)
    Pandas dataframe with the transformations performed
    '''
    df_transformed = raw_df.copy()

    # 1. drop columns that don't have data
    df_transformed.drop(columns=columns_to_drop, axis=1, inplace=True)
    logging.info(f'Columns: {columns_to_drop} have been removed: SUCCESS')

    # 2. remove the lists inside the dataframe
    columns = list_of_columns
    for col in columns:
        df_transformed[col] = df_transformed[col].apply(
            lambda x: ','.join(map(str, x)))
    logging.info(
        f'Chosen {columns} entries were transformed from lists to string: SUCCESS')

    # 3. remove the dicts inside the dataframe
    df_transformed2 = pd.json_normalize(df_transformed['jobs'])
    df_transformed2.fillna(0, inplace=True)
    logging.info('The dictionary column "jobs" has been normalized: SUCCESS')

    # 4. concat both transformations in step 2 and 3
    result = pd.concat([df_transformed, df_transformed2], axis=1).drop(
        columns_to_json_normalize, axis=1)
    logging.info('The two generated dataframes were concatenated: SUCCESS')

    return result


def transform_string_to_float(
        raw_df: pd.DataFrame,
        list_of_columns: list) -> pd.DataFrame:
    '''Function that transforms inputs from variables to floats, because many inputs that
    were supposed to be numbers contain a lot of dirt and then we clean this data

    :param raw_df: (dataframe)
    Pandas dataframe that we want to perform the transformations

    :param list_of_columns: (list)
    List of columns we want to convert from string to float

    :return: (dataframe)
    Pandas dataframe with the transformations performed
    '''
    df_transformed = raw_df.copy()

    # 1. fix columns with wrong data type
    columns = list_of_columns

    try:
        for col in columns:
            logging.info(f'Starting {col} transformation')
            df_transformed[col] = df_transformed[col].str.replace('$', '')
            df_transformed[col] = df_transformed[col].str.replace(
                ',', '').astype(float)
            logging.info(f'The {col} has been successfully transformed\n')
        logging.info(
            f'Chosen {columns} entries were transformed from string to float: SUCCESS')
        return df_transformed

    except ValueError:
        logging.info(
            f'We were unable to transform these columns {columns}, check the error: FAILED')


def transform_string_to_datetime(
        raw_df: pd.DataFrame,
        column_name: str) -> pd.DataFrame:
    '''Function that transforms variables that are as strings to datetime

    :param raw_df: (dataframe)
    Pandas dataframe that we want to perform the transformations

    :param column_name: (str)
    Column name you want to transform

    :return: (dataframe)
    Pandas dataframe with the transformations performed
    '''
    df_transformed = raw_df.copy()

    # 1. convert the variables in datetime object
    try:
        df_transformed[column_name] = pd.to_datetime(
            df_transformed[column_name], format='%b %d, %Y')
        logging.info(f'The {column_name} was transformed to datetime: SUCCESS')
        return df_transformed

    except ValueError:
        logging.info('Time data doesnt match format: FAILED')


def create_auxiliary_columns(transformed_df: pd.DataFrame) -> None:
    '''Function to create three auxiliary columns in datasets:
    "id", "created_at" and "updated_at"

    :param transformed_df: (dataframe)
    Dataframe after all transformations just
    before being inserted into database
    '''
    # inserting the "id" column
    transformed_df['id'] = pd.Series(range(1, len(transformed_df) + 1))
    logging.info(f'Column "id" was inserted: SUCCESS')

    # inserting the "created_at" and "updated_at" column
    transformed_df['created_at'] = dt.datetime.now()
    transformed_df['updated_at'] = transformed_df['created_at']
    logging.info(
        f'Columns "created_at" and "updated_at" was inserted: SUCCESS')
