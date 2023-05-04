'''
Script to download datasets from kaggle 
and read them as pandas dataframe

Author: Vitor Abdo
Date: April/2023
'''

# import necessary packages
import logging
import zipfile
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s')


def collect_from_kaggle(
        username: str, page_name:str, file_name:str, path_to_save: str) -> None:
    '''Function to connect to the Kaggle API, download 
    a given dataset and save it to a local file

    :param username: (str)
    Name of the user who uploaded the dataset

    :param page_name: (str)
    Name of the page where the dataset is included

    :param file_name: (str)
    File name of the dataset saved in the page

    :param path_to_save: (str)
    Path of the file where you want to save the downloaded dataset
    '''
    # instantiate the API
    api = KaggleApi()
    api.authenticate()
    logging.info('Authenticated API: SUCCESS')

    # Download files (datasets)
    try:
        api.dataset_download_file(
            f'{username}/{page_name}',
            file_name=file_name,
            path=path_to_save)
        logging.info(f'Downloaded {file_name} data: SUCCESS')
        
        # unzip kaggle files
        with zipfile.ZipFile(f'{path_to_save}/{file_name}.zip', 'r') as zipref:
            zipref.extractall(path=path_to_save)
        logging.info(f'Unzipped {file_name} file: SUCCESS')
    except:
        logging.info(f'Check if API prohibited the download of this dataset {file_name}: ERROR')


def read_raw_csv_data(file_path: str) -> pd.DataFrame:
    '''Load dataset as a pandas dataframe for the csv found at the path

    :param file_path: (str)
    A path to the csv

    :return: (dataframe)
    Pandas dataframe
    '''
    try:
        raw_df = pd.read_csv(file_path)
        logging.info('Execution of read_raw_csv_data: SUCCESS')
        return raw_df

    except FileNotFoundError:
        logging.error(
            "Execution of read_raw_csv_data: The file wasn't found")
        return None


def read_raw_json_data(file_path: str) -> pd.DataFrame:
    '''Load dataset as a pandas dataframe for the json found at the path

    :param file_path: (str)
    A path to the json

    :return: (dataframe)
    Pandas dataframe
    '''
    try:
        raw_df = pd.read_json(file_path)
        logging.info('Execution of read_raw_json_data: SUCCESS')
        return raw_df

    except FileNotFoundError:
        logging.error(
            "Execution of read_raw_json_data: The file wasn't found")
        return None
