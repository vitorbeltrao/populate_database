'''
Script to perform some basic data transformations 
to feed the database, without mess

Author: Vitor Abdo
Date: April/2023
'''

# import necessary packages
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s')


def transform_json_data(
        raw_df: pd.DataFrame, 
        columns_to_drop: str,
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

    :param columns_to_drop: (str)
    Name of the columns we want to delete from the dataframe

    :param list_of_columns: (list)
    List of columns we want to convert from list to string

    :param columns_to_json_normalize: (str)
    Column that we want to normalize so as not to leave it in dictionary format

    :return: (dataframe)
    Pandas dataframe with the transformations performed
    '''
    df_transformed = raw_df.copy()

    # 1. drop columns that don't have data
    df_transformed.drop(columns=[columns_to_drop], axis=1, inplace=True)
    logging.info(f'Columns: {columns_to_drop} have been removed: SUCCESS')

    # 2. remove the lists inside the dataframe
    columns = list_of_columns
    for col in columns:
        df_transformed[col] = df_transformed[col].apply(lambda x: ','.join(map(str, x)))
    logging.info('Chosen column entries were transformed from lists to string: SUCCESS')

    # 3. remove the dicts inside the dataframe
    df_transformed2 = pd.json_normalize(df_transformed['jobs'])
    df_transformed2.fillna(0, inplace=True)
    logging.info('The dictionary column has been normalized: SUCCESS')

    # 4. concat both transformations in step 2 and 3
    result = pd.concat(
        [df_transformed, df_transformed2], axis=1).drop(columns_to_json_normalize, axis=1)
    logging.info('The two generated dataframes were concatenated: SUCCESS')
    
    return result