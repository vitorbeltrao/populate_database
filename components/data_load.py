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

logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s')

# config
DB_NAME = config('DB_NAME')
USER = config('USER')
PASSWORD = config('PASSWORD')
SCHEMAS_TO_CREATE = config('SCHEMAS_TO_CREATE').split(',')


# template of connection to your postgres database
# engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')


def create_schema(
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


def create_table(
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


if __name__ == "__main__":
    # 1. create the schema if it does not already exist
    logging.info('About to start executing the create schema function')
    for schema in SCHEMAS_TO_CREATE:
        create_schema(DB_NAME, USER, PASSWORD, schema)
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
    create_table(DB_NAME, USER, PASSWORD, 'startups_hiring', 'open_positions', table_columns)
    logging.info('Done executing the create table "open_positions" function\n')

    # 2.2 create first table in "nba" schema
    logging.info('About to start executing the create table "nba_payroll" function')
    table_columns = '''
    id SERIAL PRIMARY KEY,
    team VARCHAR(30),
    season_start_year INT,
    inflation_adj_payroll FLOAT
    '''
    create_table(DB_NAME, USER, PASSWORD, 'nba', 'nba_payroll', table_columns)
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
    create_table(DB_NAME, USER, PASSWORD, 'nba', 'player_box_score_stats', table_columns)
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
    create_table(DB_NAME, USER, PASSWORD, 'nba', 'player_stats', table_columns)
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
    create_table(DB_NAME, USER, PASSWORD, 'nba', 'nba_salaries', table_columns)
    logging.info('Done executing the create table "nba_salaries" function\n')