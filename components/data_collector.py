'''
Script to upload the original files
to the raw layer of the data lake

Author: Vitor Abdo
Date: April/2023
'''

# import necessary packages
import logging
import pandas as pd
from decouple import config

logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s')

# config
CSV_PATH = config('CSV_PATH')
JSON_PATH = config('JSON_PATH')


def collect_raw_csv_data(file_path: str) -> pd.DataFrame:
    '''Load dataset as a pandas dataframe for the csv found at the path

    :param file_path: (str)
    A path to the csv

    :return: (dataframe)
    Pandas dataframe
    '''
    try:
        raw_df = pd.read_csv(file_path)
        logging.info('Execution of collect_raw_csv_data: SUCCESS')
        return raw_df

    except FileNotFoundError:
        logging.error(
            "Execution of collect_raw_csv_data: The file wasn't found")
        return None


def collect_raw_json_data(file_path: str) -> pd.DataFrame:
    '''Load dataset as a pandas dataframe for the json found at the path

    :param file_path: (str)
    A path to the json

    :return: (dataframe)
    Pandas dataframe
    '''
    try:
        raw_df = pd.read_json(file_path)
        logging.info('Execution of collect_raw_json_data: SUCCESS')
        return raw_df

    except FileNotFoundError:
        logging.error(
            "Execution of collect_raw_json_data: The file wasn't found")
        return None
