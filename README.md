# Populate PostgreSQL Database

## Table of Contents

1. [Project Description](#description)
2. [Files Description](#files)
3. [Running Files](#running)
4. [Orchestration](#orchestration)
5. [Licensing and Authors](#licensingandauthors)
***

## Project Description <a name="description"></a>

This project aims to populate a PostgreSQL database with data from two distinct sources on Kaggle. Are they:

* [Top Tech Startups Hiring 2023](https://www.kaggle.com/datasets/chickooo/top-tech-startups-hiring-2023?select=json_data.json)
* [NBA Players & Team Data](https://www.kaggle.com/datasets/loganlauton/nba-players-and-team-data)

This data from the above source was collected directly by the Kaggle API, where we created a function to get it, to see the code see the *data_collector.py* component and the *main.py file*. For more information on how to use the Kaggle API I recommend the [documentation](https://www.kaggle.com/docs/api) and this other [source](https://python.plainenglish.io/how-to-use-the-kaggle-api-in-python-4d4c812c39c7).

The first source, referring to startups, only a .json file was obtained; The second source, about NBA teams and players, four .csv files were obtained. The data is collected, transformed, and loaded into the database using a modularized approach. 

![Populate Database architecture](https://github.com/vitorbeltrao/populate_database/blob/main/images/populate_db_architecture.jpg?raw=true)

In the end, a database was created that has two schemas (one for each data source) and within each schema we have all the necessary tables. See the entire database hierarchy:

![Populate Database architecture](https://github.com/vitorbeltrao/populate_database/blob/main/images/database_hierarchy.jpg?raw=true)
***

## Files Description <a name="files"></a>

* `docker-compose.yml`: Docker Compose file for creating the PostgreSQL database locally.

* `main.py`: Main Python script for running the data collection, transformation, and upload the transformed data, that is, all three components created in the *components* folder.

* `components/`: Directory containing the modularized components for the project.

    * `data_collector.py`: Python module to collect raw data from Kaggle and read it as a pandas dataframe.
    * `data_transform.py`: Python module for transforming the raw data into a format that can be loaded into the PostgreSQL database.
    * `data_load.py`: Python module for loading the transformed data into the PostgreSQL database.

* `.env`: File containing environment variables used in the project.
***

## Running Files <a name="running"></a>

To run the project, follow these steps:

### Clone the repository

Go to [populate_database](https://github.com/vitorbeltrao/populate_database) and click on Fork in the upper right corner. This will create a fork in your Github account, i.e., a copy of the repository that is under your control. Now clone the repository locally so you can start working on it:

`git clone https://github.com/[your_github_username]/populate_database.git`

and go into the repository:

`cd populate_database` 

### Install Docker

On Windows, you will have to [install docker](https://docs.docker.com/desktop/install/windows-install/) to be able to run the database locally.

Once installed, you can run it in your terminal: `docker-compose up db` or `docker-compose up -d db` to start your database. To stop docker, just run it in your terminal: `docker-compose down`.

### .env File

To make everything work, you need to create the `.env` file in your main directory, so that *main.py* runs smoothly. 

In the .env, you must define the following variables:

* `HOST_NAME`: str (Name of your hostname)

* `PORT`: str (Number of port used for the protocol)

* `DB_NAME`: str (Name of your postgres database)

* `USER`: str (Name of postgres user)

* `PASSWORD`: str (The password of created database)

* `SCHEMAS_TO_CREATE`: Do not quote this variable (Name of the schemas you want to create, for example: *SCHEMAS_TO_CREATE = startups_hiring,nba*)

* `OPEN_POSITIONS_RAW_PATH`: str (Startups dataset path)

* `NBA_PAYROLL_RAW_PATH`, `NBA_PLAYER_BOX_RAW_PATH`, `NBA_PLAYER_STATS_RAW_PATH`, `NBA_SALARIES_RAW_PATH`: str (NBA datasets path)

### main.py File

After all the above steps, and with docker running, you can run it in your terminal, in your main directory: `python main.py` to execute the three components in order from the *components* folder.
***

## Orchestration <a name="orchestration"></a>

The project uses a modularized approach for data collection, transformation, and loading. This approach allows for greater flexibility and scalability in the project. The main.py script orchestrates the execution of these modules.
***

## Licensing and Author <a name="licensingandauthors"></a>

Vítor Beltrão - Data Scientist

Reach me at: 

- vitorbeltraoo@hotmail.com

- [linkedin](https://www.linkedin.com/in/v%C3%ADtor-beltr%C3%A3o-56a912178/)

- [github](https://github.com/vitorbeltrao)

- [medium](https://pandascouple.medium.com)

Licensing: [MIT LICENSE](https://github.com/vitorbeltrao/populate_database/blob/main/LICENSE)