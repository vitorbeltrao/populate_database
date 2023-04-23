'''
File to create the schemas, tables and populate 
them inside my postgres database

Author: Vitor Abdo
Date: April/2023
'''

# import necessary packages
import logging
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s')


# template of connection to your postgres database
# engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name')
engine = create_engine('postgresql+psycopg2://sanya:root@localhost/sanya_db')
print(engine)