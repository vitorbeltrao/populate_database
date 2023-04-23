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
NEW_DB_NAME = config('NEW_DB_NAME')
USER = config('USER')
PASSWORD = config('PASSWORD')
ENGINE = config('ENGINE')

# template of connection to your postgres database
# engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')


def create_database(newdb_name, user, password):
    # conectando ao servidor PostgreSQL
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        dbname=newdb_name,
        user=user,
        password=password
    )

    # definindo a conexão para o modo autocommit
    conn.autocommit = True

    # verificando se o banco de dados já existe
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (newdb_name,))
    exists = bool(cur.rowcount)
    if exists:
        print(f"The database '{newdb_name}' already exists.")
    else:
        # criando o banco de dados
        cur.execute(f"CREATE DATABASE {newdb_name}")
        print(f"The database '{newdb_name}' has been created.")

    # finalizando a conexão
    cur.close()
    conn.close()


if __name__ == "__main__":
    logging.info('About to start executing the create_engine function\n')

    create_database(NEW_DB_NAME, USER, PASSWORD)

    logging.info('Done executing the function')
