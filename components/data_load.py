'''
File to create the schemas, tables and populate 
them inside my postgres database

Author: Vitor Abdo
Date: April/2023
'''

# import necessary packages
import logging
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from decouple import config

from data_collector import collect_raw_json_data
from data_transform import transform_json_data

logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s')

# config
DB_NAME = config('DB_NAME')
USER = config('USER')
PASSWORD = config('PASSWORD')
SCHEMAS_TO_CREATE = config('SCHEMAS_TO_CREATE').split(',')
OPEN_POSITIONS_RAW_PATH = config('OPEN_POSITIONS_RAW_PATH')


def create_schema_into_postgresql(
        db_name: str, 
        user_name: str, 
        password: str, 
        schema_name: str) -> None:
    '''Connects to a PostgreSQL database and creates a schema if it does not already exist

    :param db_name: (str)
    The name of the database to connect to

    :param user_name: (str)
    The name of the user to authenticate as

    :param password: (str) 
    The user's password

    :param schema_name: (str)
    The name of the schema to create
    '''

    # Set up the connection
    conn = psycopg2.connect(
        host='localhost',
        database=db_name,
        user=user_name,
        password=password
    )

    # Create a cursor to execute SQL commands
    cur = conn.cursor()

    # Execute a query to check if the schema already exists
    cur.execute(f'SELECT schema_name FROM information_schema.schemata WHERE schema_name = {schema_name}')
    result = cur.fetchone()

    # If the schema does not exist, create it
    if not result:
        cur.execute(f'CREATE SCHEMA {schema_name}')
        logging.info(f'Schema {schema_name} created successfully')
    else:
        logging.info(f'Schema {schema_name} already exists')

    # Commit and close the connection
    conn.commit()
    cur.close()
    conn.close()


def create_table_into_postgresql(
        db_name: str,
        user_name: str, 
        password: str, 
        schema_name: str, 
        table_name: str, 
        table_columns: str) -> None:
    '''Function that creates a table if it does not exist in a PostgresSQL schema

    :param db_name: (str)
    The name of the database to connect to

    :param user_name: (str)
    The name of the user to authenticate as

    :param password: (str) 
    The user's password

    :param schema_name: (str)
    The name of the schema where the table should be created

    :param table_name: (str)
    The name of the table to be created

    :param table_columns: (str)
    The columns definition of the table in the format "column_name DATA_TYPE, column_name DATA_TYPE, ..."
    '''
    # Connection to the PostgresSQL database
    conn = psycopg2.connect(
        host='localhost',
        database=db_name,
        user=user_name,
        password=password,
        port='5432'
    )

    # Creation of a cursor to execute SQL commands
    cur = conn.cursor()

    # Check if the table exists in the schema
    cur.execute(
        'SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = %s AND table_name = %s)', 
        (schema_name, table_name))
    exists = cur.fetchone()[0]

    # If the table does not exist, create the table
    if not exists:
        create_table_query = f'CREATE TABLE {schema_name}.{table_name} ({table_columns})'
        cur.execute(create_table_query)
        logging.info(f'The table {table_name} was created in the {schema_name} schema')
    else:
        logging.info(f'The table {table_name} already exists in the {schema_name} schema')

    # Commit changes and close the connection
    conn.commit()
    cur.close()
    conn.close()


def insert_data_into_postgresql(
    datab_name: str,
    user_name: str, 
    password: str, 
    schema_name: str, 
    table_name: str, 
    df: pd.DataFrame) -> None:
    '''
    Function that inserts data from a Pandas DataFrame into a PostgreSQL table.
    If the table does not exist, it creates a new one in the specified schema.

    :param datab_name: (str)
    The name of the database to connect to.

    :param user_name: (str)
    The name of the user to authenticate as.

    :param password: (str) 
    The user's password.

    :param schema_name: (str)
    The name of the schema where the table should be created.

    :param table_name: (str)
    The name of the table to be created or where the data will be inserted.

    :param df: (pandas.DataFrame)
    The DataFrame containing the data to be inserted.
    '''

    # Connect to the PostgreSQL database
    db_host = 'localhost'
    db_port = '5432'
    db_name = datab_name
    db_user = user_name
    db_pass = password

    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_pass
    )
    # create engine
    engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

    # Create a temporary table with the data from the DataFrame
    temp_table_name = f'temp_{table_name}'
    df.to_sql(name=temp_table_name, con=engine.connect(), schema=schema_name, index=False, if_exists='replace')
    logging.info('Temporary table was created: SUCCESS')

    # Check if the final table exists
    table_exists = engine.has_table(table_name=table_name, schema=schema_name)

    if table_exists:
        # Check if the DataFrame columns match the table columns
        db_cols_query = f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}' AND table_schema='{schema_name}'"
        with conn.cursor() as cur:
            cur.execute(db_cols_query)
            db_columns = [col[0] for col in cur.fetchall()]

        df_columns = df.columns.tolist()

        if db_columns != df_columns:
            raise ValueError(f'The columns of the DataFrame do not match the columns of the table {schema_name}.{table_name}')

        # Insert the data into the final table without overwriting existing data
        insert_query = f'INSERT INTO {schema_name}.{table_name} SELECT * FROM {schema_name}.{temp_table_name} ON CONFLICT DO NOTHING;'
        with conn.cursor() as cur:
            cur.execute(insert_query)
        logging.info('The dataframe data has been inserted: SUCCESS')

    # Remove the temporary table
    drop_query = f'DROP TABLE {schema_name}.{temp_table_name};'
    with conn.cursor() as cur:
        cur.execute(drop_query)
    logging.info('The temp table has been removed: SUCCESS')

    # Close the database connection
    conn.commit()
    conn.close()


if __name__ == "__main__":
    # 1. create the schema if it does not already exist
    logging.info('About to start executing the create schema function')
    for schema in SCHEMAS_TO_CREATE:
        create_schema_into_postgresql(DB_NAME, USER, PASSWORD, schema)
    logging.info('Done executing the create schema function\n')

    # 2. create tables
    # 2.1 create first table in "startups_hiring" schema
    logging.info('About to start executing the create table "open_positions" function')
    table_columns = '''
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(50),
    headline VARCHAR(500),
    tags VARCHAR(500),
    website VARCHAR(100),
    employees VARCHAR(50),
    about VARCHAR(1000),
    locations VARCHAR(500),
    industries VARCHAR(500),
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
    create_table_into_postgresql(DB_NAME, USER, PASSWORD, 'startups_hiring', 'open_positions', table_columns)
    logging.info('Done executing the create table "open_positions" function\n')

    # 2.2 create first table in "nba" schema
    logging.info('About to start executing the create table "nba_payroll" function')
    table_columns = '''
    id SERIAL PRIMARY KEY,
    team VARCHAR(30),
    season_start_year INT,
    inflation_adj_payroll FLOAT
    '''
    create_table_into_postgresql(DB_NAME, USER, PASSWORD, 'nba', 'nba_payroll', table_columns)
    logging.info('Done executing the create table "nba_payroll" function\n')

    # 2.2 create second table in "nba" schema
    logging.info('About to start executing the create table "player_box_score_stats" function')
    table_columns = '''
    id SERIAL PRIMARY KEY,
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
    create_table_into_postgresql(DB_NAME, USER, PASSWORD, 'nba', 'player_box_score_stats', table_columns)
    logging.info('Done executing the create table "player_box_score_stats" function\n')

    # 2.3 create third table in "nba" schema
    logging.info('About to start executing the create table "player_stats" function')
    table_columns = '''
    id SERIAL PRIMARY KEY,
    player_box_score_id INT,
    season INT,
    player_name VARCHAR(30),
    pos VARCHAR(5),
    age INT,
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
    minus FLOAT
    '''
    create_table_into_postgresql(DB_NAME, USER, PASSWORD, 'nba', 'player_stats', table_columns)
    logging.info('Done executing the create table "player_stats" function\n')

    # 2.4 create fourth table in "nba" schema
    logging.info('About to start executing the create table "nba_salaries" function')
    table_columns = '''
    id SERIAL PRIMARY KEY,
    player_stats_id INT,
    player_name VARCHAR(30),
    season_start_year INT,
    salary FLOAT,
    inflation_adj_salary FLOAT
    '''
    create_table_into_postgresql(DB_NAME, USER, PASSWORD, 'nba', 'nba_salaries', table_columns)
    logging.info('Done executing the create table "nba_salaries" function\n')

    # 3. insert transformed dataframes into postgres
    # 3.1 insert data into open_positions table
    logging.info('About to start inserting the data into open_positions table')

    # extracting data
    open_positions_raw_df = collect_raw_json_data(OPEN_POSITIONS_RAW_PATH)

    # transforming data
    list_of_columns = ['tags', 'locations', 'industries']
    open_positions_transformed_df = transform_json_data(
        open_positions_raw_df, 'logo_url', list_of_columns, 'jobs')
    
    # loading data
    insert_data_into_postgresql(
        DB_NAME, USER, PASSWORD, 'startups_hiring', 'open_positions', open_positions_transformed_df)
    logging.info('Done executing inserting the data into open_positions table\n')

    # 3.2 insert data into ... table