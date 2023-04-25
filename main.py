'''
'''

# import necessary packages






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