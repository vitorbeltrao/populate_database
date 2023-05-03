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
from sqlalchemy import create_engine, inspect

logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s')


def create_schema_into_postgresql(
        host_name: str,
        db_name: str,
        user_name: str,
        password: str,
        schema_name: str) -> None:
    '''Connects to a PostgreSQL database and creates a schema if it does not already exist

    :param host_name: (str)
    Is the network name for the physical machine on which the node is installed

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
        host=host_name,
        database=db_name,
        user=user_name,
        password=password
    )

    # Create a cursor to execute SQL commands
    cur = conn.cursor()

    # Execute a query to check if the schema already exists
    cur.execute(
        f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}'")
    result = cur.fetchone()

    # If the schema does not exist, create it
    if not result:
        cur.execute(f"CREATE SCHEMA {schema_name}")
        logging.info(f"Schema {schema_name} created successfully")
    else:
        logging.info(f"Schema {schema_name} already exists")

    # Commit and close the connection
    conn.commit()
    cur.close()
    conn.close()


def create_table_into_postgresql(
        host_name: str,
        port: str,
        db_name: str,
        user_name: str,
        password: str,
        schema_name: str,
        table_name: str,
        table_columns: str) -> None:
    '''Function that creates a table if it does not exist in a PostgresSQL schema

    :param host_name: (str)
    Is the network name for the physical machine on which the node is installed

    :param port: (str)
    Default port used for the protocol

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
        host=host_name,
        database=db_name,
        user=user_name,
        password=password,
        port=port
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
        logging.info(
            f'The table {table_name} was created in the {schema_name} schema')
    else:
        logging.info(
            f'The table {table_name} already exists in the {schema_name} schema')

    # Commit changes and close the connection
    conn.commit()
    cur.close()
    conn.close()


def insert_data_into_postgresql(
        host_name: str,
        port: str,
        datab_name: str,
        user_name: str,
        password: str,
        schema_name: str,
        table_name: str,
        df: pd.DataFrame) -> None:
    '''
    Function that inserts data from a Pandas DataFrame into a PostgreSQL table.
    If the table does not exist, it creates a new one in the specified schema.

    :param host_name: (str)
    Is the network name for the physical machine on which the node is installed

    :param port: (str)
    Default port used for the protocol

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
    db_host = host_name
    db_port = port
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
    engine = create_engine(
        f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

    # Create a temporary table with the data from the DataFrame
    temp_table_name = f'temp_{table_name}'
    df.to_sql(
        name=temp_table_name,
        con=engine.connect(),
        schema=schema_name,
        index=False,
        if_exists='replace')
    logging.info('Temporary table was created: SUCCESS')

    # Check if the final table exists
    inspector = inspect(engine)
    table_exists = inspector.has_table(table_name, schema=schema_name)

    if table_exists:
        # Check if the DataFrame columns match the table columns
        db_cols_query = f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}' AND table_schema='{schema_name}'"
        with conn.cursor() as cur:
            cur.execute(db_cols_query)
            db_columns = [col[0] for col in cur.fetchall()]

        df_columns = df.columns.tolist()

        if db_columns != df_columns:
            raise ValueError(
                f'The columns of the DataFrame do not match the columns of the table {schema_name}.{table_name}')

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


def add_auto_increment_id_to_table(
        host_name: str, db_name: str, user_name: str, password: str, schema_table: str) -> None:
    '''Connects to a PostgreSQL database and adds an 
    auto-incrementing id column to a specified table

    :param host_name: (str)
    Is the network name for the physical machine on which the node is installed

    :param db_name: (str)
    The name of the database to connect to

    :param user_name: (str)
    The name of the user to authenticate as

    :param password: (str)
    The user's password

    :param schema_table: (str)
    The name of the schema and table in database.
    Example: 'nba.nba_payroll'
    '''
    # Connect to the database
    conn = psycopg2.connect(
        host=host_name,
        database=db_name,
        user=user_name,
        password=password
    )

    # Add a new column with an auto-incrementing id
    cur = conn.cursor()
    cur.execute(f'ALTER TABLE {schema_table} ADD COLUMN id SERIAL PRIMARY KEY;')
    conn.commit()
    logging.info(f'The ids in the {schema_table} were created: SUCCESS')

    # Close the database connection
    cur.close()
    conn.close()


def add_monitoring_columns_to_table(
        host_name: str, db_name: str, user_name: str, password: str, schema_table: str) -> None:
    '''Connects to a PostgreSQL database and adds two
    columns "created_at" and "updated_at" to monitore
    the flow of data

    :param host_name: (str)
    Is the network name for the physical machine on which the node is installed

    :param db_name: (str)
    The name of the database to connect to

    :param user_name: (str)
    The name of the user to authenticate as

    :param password: (str)
    The user's password

    :param schema_table: (str)
    The name of the schema and table in database.
    Example: 'nba.nba_payroll'
    '''
    # Connect to the database
    conn = psycopg2.connect(
        host=host_name,
        database=db_name,
        user=user_name,
        password=password
    )

    # Add two new columns to monitor the data "created_at" and "updated_at"
    cur = conn.cursor()
    cur.execute(
        f'''ALTER TABLE {schema_table} ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ADD COLUMN updated_at TIMESTAMP DEFAULT now();''')
    conn.commit()
    logging.info(f'The monitoring columns in the {schema_table} were created: SUCCESS')

    # Close the database connection
    cur.close()
    conn.close()
