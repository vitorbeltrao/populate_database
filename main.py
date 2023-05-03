'''
Main file that will run all the components in order to
insert the data from the tables in the database

Author: Vitor Abdo
Date: April/2023
'''

# import necessary packages
import logging
from decouple import config

# data_collector component
from components.data_collector import collect_from_kaggle
from components.data_collector import read_raw_json_data
from components.data_collector import read_raw_csv_data

# data_transform component
from components.data_transform import transform_json_data
from components.data_transform import transform_string_to_float
from components.data_transform import transform_string_to_datetime

# data_load component
from components.data_load import create_schema_into_postgresql
from components.data_load import create_table_into_postgresql
from components.data_load import insert_data_into_postgresql
from components.data_load import add_auto_increment_id_to_table
from components.data_load import add_monitoring_columns_to_table

logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s')

# config
HOST_NAME = config('HOST_NAME')
PORT = config('PORT')
DB_NAME = config('DB_NAME')
USER = config('USER')
PASSWORD = config('PASSWORD')
SCHEMAS_TO_CREATE = config('SCHEMAS_TO_CREATE').split(',')
OPEN_POSITIONS_RAW_PATH = config('OPEN_POSITIONS_RAW_PATH')
NBA_PAYROLL_RAW_PATH = config('NBA_PAYROLL_RAW_PATH')
NBA_PLAYER_BOX_RAW_PATH = config('NBA_PLAYER_BOX_RAW_PATH')
NBA_PLAYER_STATS_RAW_PATH = config('NBA_PLAYER_STATS_RAW_PATH')
NBA_SALARIES_RAW_PATH = config('NBA_SALARIES_RAW_PATH')


if __name__ == "__main__":
    # 0. download the Kaggle API files
    logging.info('About to start executing Kaggle files download')

    # 0.1 download startup data
    collect_from_kaggle(
        'chickooo', 'top-tech-startups-hiring-2023', 'json_data.json', './data')
    
    # 0.2 download nba data
    nba_datasets_list = [
        'NBA Payroll(1990-2023).csv', 
        'NBA Player Box Score Stats(1950 - 2022).csv', 
        'NBA Player Stats(1950 - 2022).csv', 
        'NBA Salaries(1990-2023).csv']
    for nba_dataset in nba_datasets_list:
        collect_from_kaggle(
            'loganlauton', 'nba-players-and-team-data', nba_dataset, './data')

    # 1. create the schema if it does not already exist
    logging.info('About to start executing the create schema function')
    for schema in SCHEMAS_TO_CREATE:
        create_schema_into_postgresql(HOST_NAME, DB_NAME, USER, PASSWORD, schema)
    logging.info('Done executing the create schema function\n')

    # 2. create tables
    # 2.1 create first table in "startups_hiring" schema
    logging.info(
        'About to start executing the create table "open_positions" function')
    table_columns = '''
    company_name VARCHAR(50),
    headline TEXT,
    tags TEXT,
    website TEXT,
    employees VARCHAR(50),
    about TEXT,
    locations TEXT,
    industries TEXT,
    engineering INT,
    founder INT,
    investor INT,
    marketing INT,
    other_engineering INT,
    product INT,
    sales INT,
    designer INT,
    management INT,
    operations INT
    '''
    create_table_into_postgresql(
        HOST_NAME,
        PORT,
        DB_NAME,
        USER,
        PASSWORD,
        'startups_hiring',
        'open_positions',
        table_columns)
    logging.info('Done executing the create table "open_positions" function\n')

    # 2.2 create first table in "nba" schema
    logging.info(
        'About to start executing the create table "nba_payroll" function')
    table_columns = '''
    team VARCHAR(30),
    season_start_year INT,
    payroll FLOAT,
    inflation_adj_payroll FLOAT
    '''
    create_table_into_postgresql(
        HOST_NAME,
        PORT,
        DB_NAME,
        USER,
        PASSWORD,
        'nba',
        'nba_payroll',
        table_columns)
    logging.info('Done executing the create table "nba_payroll" function\n')

    # 2.2 create second table in "nba" schema
    logging.info(
        'About to start executing the create table "player_box_score_stats" function')
    table_columns = '''
    season INT,
    game_id INT,
    player_name VARCHAR(30),
    team VARCHAR(30),
    game_date DATE,
    matchup VARCHAR(20),
    wl VARCHAR (5),
    min INT,
    fgm INT,
    fga FLOAT,
    fg_pct FLOAT,
    fg3m FLOAT,
    fg3a FLOAT,
    fg3_pct FLOAT,
    ftm INT,
    fta FLOAT,
    ft_pct FLOAT,
    oreb FLOAT,
    dreb FLOAT,
    reb FLOAT,
    ast FLOAT,
    stl FLOAT,
    blk FLOAT,
    tov FLOAT,
    pf FLOAT,
    pts INT,
    plus_minus FLOAT,
    video_available INT
    '''
    create_table_into_postgresql(
        HOST_NAME,
        PORT,
        DB_NAME,
        USER,
        PASSWORD,
        'nba',
        'player_box_score_stats',
        table_columns)
    logging.info(
        'Done executing the create table "player_box_score_stats" function\n')

    # 2.3 create third table in "nba" schema
    logging.info(
        'About to start executing the create table "player_stats" function')
    table_columns = '''
    season INT,
    player_name VARCHAR(30),
    pos VARCHAR(10),
    age INT,
    tm VARCHAR(10),
    g FLOAT,
    gs FLOAT,
    mp FLOAT,
    fg FLOAT,
    fga FLOAT,
    fg_percent FLOAT,
    threep FLOAT,
    threepa FLOAT,
    threep_percent FLOAT,
    twop FLOAT,
    twopa FLOAT,
    twop_percent FLOAT,
    efg_percent FLOAT,
    ft FLOAT,
    fta FLOAT,
    ft_percent FLOAT,
    orb FLOAT,
    drb FLOAT,
    trb FLOAT,
    ast FLOAT,
    stl FLOAT,
    blk FLOAT,
    tov FLOAT,
    pf FLOAT,
    pts FLOAT
    '''
    create_table_into_postgresql(
        HOST_NAME,
        PORT,
        DB_NAME,
        USER,
        PASSWORD,
        'nba',
        'player_stats',
        table_columns)
    logging.info('Done executing the create table "player_stats" function\n')

    # 2.4 create fourth table in "nba" schema
    logging.info(
        'About to start executing the create table "nba_salaries" function')
    table_columns = '''
    player_name VARCHAR(30),
    season_start_year INT,
    salary FLOAT,
    inflation_adj_salary FLOAT
    '''
    create_table_into_postgresql(
        HOST_NAME,
        PORT,
        DB_NAME,
        USER,
        PASSWORD,
        'nba',
        'nba_salaries',
        table_columns)
    logging.info('Done executing the create table "nba_salaries" function\n')

    # 3. insert transformed dataframes into postgres
    # 3.1 insert data into open_positions table
    logging.info('About to start inserting the data into open_positions table')

    # extracting data
    open_positions_raw_df = read_raw_json_data(OPEN_POSITIONS_RAW_PATH)

    # transforming data
    columns_to_drop = ['id', 'logo_url']
    columns_to_convert_to_str = ['tags', 'locations', 'industries']
    open_positions_transformed_df = transform_json_data(
        open_positions_raw_df, columns_to_drop, columns_to_convert_to_str, 'jobs')

    open_positions_transformed_df = open_positions_transformed_df.rename(
        columns=lambda x: x.strip().lower().replace(' ', '_'))  # standardize column names

    open_positions_transformed_df.drop_duplicates(inplace=True)

    # loading data
    insert_data_into_postgresql(
        HOST_NAME,
        PORT,
        DB_NAME,
        USER,
        PASSWORD,
        'startups_hiring',
        'open_positions',
        open_positions_transformed_df)
    logging.info(
        'Done executing inserting the data into open_positions table\n')

    # 3.2 insert data into nba_payroll table
    logging.info('About to start inserting the data into nba_payroll table')

    # extracting data
    nba_payroll_raw_df = read_raw_csv_data(NBA_PAYROLL_RAW_PATH)

    # transforming data
    columns_to_convert_to_float = ['payroll', 'inflationAdjPayroll']
    nba_payroll_transformed_df = transform_string_to_float(
        nba_payroll_raw_df, columns_to_convert_to_float)

    nba_payroll_transformed_df.drop(
        ['Unnamed: 0'],
        axis=1,
        inplace=True)  # drop unnecessary columns

    nba_payroll_transformed_df = nba_payroll_transformed_df.rename(
        columns=lambda x: x.strip().lower().replace(
            ' ', '_'))  # standardize column names
    nba_payroll_transformed_df.rename(
        columns={
            'seasonstartyear': 'season_start_year',
            'inflationadjpayroll': 'inflation_adj_payroll'},
        inplace=True)  # standardize column names

    nba_payroll_transformed_df.drop_duplicates(inplace=True)

    # loading data
    insert_data_into_postgresql(
        HOST_NAME,
        PORT,
        DB_NAME,
        USER,
        PASSWORD,
        'nba',
        'nba_payroll',
        nba_payroll_transformed_df)
    logging.info('Done executing inserting the data into nba_payroll table\n')

    # 3.3 insert data into player_box_score_stats table
    logging.info(
        'About to start inserting the data into player_box_score_stats table')

    # extracting data
    nba_player_box_raw_df = read_raw_csv_data(NBA_PLAYER_BOX_RAW_PATH)

    # transforming data
    column_to_convert_to_date = 'GAME_DATE'
    nba_player_box_transformed_df = transform_string_to_datetime(
        nba_player_box_raw_df, column_to_convert_to_date)

    nba_player_box_transformed_df.drop(
        ['Unnamed: 0'], axis=1, inplace=True)  # drop unnecessary columns

    nba_player_box_transformed_df = nba_player_box_transformed_df.rename(
        columns=lambda x: x.strip().lower().replace(' ', '_'))  # standardize column names

    nba_player_box_transformed_df.drop_duplicates(inplace=True)

    # loading data
    insert_data_into_postgresql(
        HOST_NAME,
        PORT,
        DB_NAME,
        USER,
        PASSWORD,
        'nba',
        'player_box_score_stats',
        nba_player_box_transformed_df)
    logging.info(
        'Done executing inserting the data into player_box_score_stats table\n')

    # 3.4 insert data into player_stats table
    logging.info('About to start inserting the data into player_stats table')

    # extracting data
    nba_player_stats_raw_df = read_raw_csv_data(NBA_PLAYER_STATS_RAW_PATH)

    # transforming data
    nba_player_stats_transformed_df = nba_player_stats_raw_df.drop(
        ['Unnamed: 0.1', 'Unnamed: 0'], axis=1)  # drop unnecessary columns

    nba_player_stats_transformed_df = nba_player_stats_transformed_df.rename(
        columns=lambda x: x.strip().lower().replace(' ', '_'))  # standardize column names
    nba_player_stats_transformed_df.rename(
        columns={
            'player': 'player_name',
            'fg%': 'fg_percent',
            '3p': 'threep',
            '3pa': 'threepa',
            '3p%': 'threep_percent',
            '2p': 'twop',
            '2pa': 'twopa',
            '2p%': 'twop_percent',
            'efg%': 'efg_percent',
            'ft%': 'ft_percent'}, inplace=True)  # standardize column names

    nba_player_stats_transformed_df.drop_duplicates(inplace=True)

    # loading data
    insert_data_into_postgresql(
        HOST_NAME,
        PORT,
        DB_NAME,
        USER,
        PASSWORD,
        'nba',
        'player_stats',
        nba_player_stats_transformed_df)
    logging.info('Done executing inserting the data into player_stats table\n')

    # 3.5 insert data into nba_salaries table
    logging.info('About to start inserting the data into nba_salaries table')

    # extracting data
    nba_salaries_raw_df = read_raw_csv_data(NBA_SALARIES_RAW_PATH)

    # transforming data
    columns_to_convert_to_float = ['salary', 'inflationAdjSalary']
    nba_salaries_transformed_df = transform_string_to_float(
        nba_salaries_raw_df, columns_to_convert_to_float)

    nba_salaries_transformed_df.drop(
        ['Unnamed: 0'],
        axis=1,
        inplace=True)  # drop unnecessary columns

    nba_salaries_transformed_df = nba_salaries_transformed_df.rename(
        columns=lambda x: x.strip().lower().replace(
            ' ', '_'))  # standardize column names
    nba_salaries_transformed_df.rename(
        columns={
            'playername': 'player_name',
            'seasonstartyear': 'season_start_year',
            'inflationadjsalary': 'inflation_adj_salary'},
        inplace=True)  # standardize column names

    nba_salaries_transformed_df.drop_duplicates(inplace=True)

    # loading data
    insert_data_into_postgresql(
        HOST_NAME,
        PORT,
        DB_NAME,
        USER,
        PASSWORD,
        'nba',
        'nba_salaries',
        nba_salaries_transformed_df)
    logging.info('Done executing inserting the data into nba_salaries table\n')

    # 4. Create unique id's incrementally in tables already inserted in postgres
    # 5. Create monitoring columns in tables already inserted in postgres 
    logging.info(
        'About to start to create unique ids and monitoring columns for the tables')
    schema_tables = [
        'startups_hiring.open_positions',
        'nba.nba_payroll',
        'nba.player_box_score_stats',
        'nba.player_stats',
        'nba.nba_salaries']
    for i in schema_tables:
        add_auto_increment_id_to_table(HOST_NAME, DB_NAME, USER, PASSWORD, i)
        add_monitoring_columns_to_table(HOST_NAME, DB_NAME, USER, PASSWORD, i)
    logging.info(
        'Done executing the creation of unique ids and monitoring columns')
