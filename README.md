# Multilevel data analytics serivce
### A Data Engineering Capstone project

The purpose of this service is to regularly gather data from two Eurostat datasets, and make that data available from a database along with data from the European Social Survey (ESS). Since these two data sources have IDs on the regional level that uses the NUTS classification (Nomenclature of territorial units for statistics), they can be joined so that the Eurostat data acts as contextual for the survey data.

## Description
The projects sets up an Apache Airflow server, with three jobs  (Directed Acyclic Graphs/DAGs). Two of these are recurring DAGS, one is run on-demand.

The two recurring DAGs downloads data using the [Eurostat JSON & UNICODE Web Services](https://ec.europa.eu/eurostat/web/json-and-unicode-web-services) API, checks the quality of that data, stages it in an AWS S3 bucket and finally copies it into a Redshift database.

The on-demand DAG copies ESS survey data from a .dta file (included in this repository) to Amazon S3 and from there to Redshift.

The Airflow server is run using Docker Compose. Note that the requirments.txt file in this repository is not used in the actual DAGs, but only contains tools used in development such as linters. A fair deal of the work in the DAGs is performed by PythonOperators. In the future, these tasks could be moved to [Docker operators](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html) for better handling of dependencies.

## Steps
The following steps were taken in completing the project:
- Figuring out the Eurostat API and write a downloader which takes datasets codes as input and downloads the data.
- Inspect the data from the European Social Survey to see if it can be joined with Eurostat data on the regional level (it can).
- Move ad-hoc code into formal DAGs, with concrete steps for data quality checks.
- Create SQL database schemas for the data, using local Postgres database for testing.
- Provision AWS services (S3 and Redshift), and implement tasks to move downloaded data into the cloud.

## Purpose
The purposed of this project is to populate a database which would then suitable for use by an analytics service, for instance a web site. Since Amazon Redshift is an OLAP type database, it is suited for read-only queries which gather large amounts of data, and joins them, across tables. The Eurostat data is subject to change without notice, and without access to the replaced data, so an analytics table containing data backwards in time can be very useful.

## Data model
The data model is based on first splitting up the Eurostat data by the values on an indicator variable. This keeps the data size managable, and below the max limit of the Eurostat API. While on disk, the data is stored as separate .csv files. When fed into the analytics database, the data is stored in three tables,  one for the ESS data and one each for the two Eurostat data sets.


### Data set 1
Population on 1 January by age group, sex and NUTS 3 region

Name of DAG: `demo_r_pjangrp3`

|    | indic_de   | unit   | geo   |   time |   value | downloaded_at              |
|---:|:-----------|:-------|:------|-------:|--------:|:---------------------------|
|  0 | DEPRATIO1  | PC     | AL    |   2014 |    46.1 | 2021-01-15 16:59:59.706976 |
|  1 | DEPRATIO1  | PC     | AL    |   2015 |    45.9 | 2021-01-15 16:59:59.706976 |
|  2 | DEPRATIO1  | PC     | AL    |   2016 |    45.5 | 2021-01-15 16:59:59.706976 |
|  3 | DEPRATIO1  | PC     | AL    |   2017 |    45.5 | 2021-01-15 16:59:59.706976 |
|  4 | DEPRATIO1  | PC     | AL    |   2018 |    45.4 | 2021-01-15 16:59:59.706976 |

This dataset contains 764 463 rows of data.

### Data set 2
Population: Structure indicators by NUTS 3 region

Name of DAG: `demo_r_pjanind3`
|    | indic_de   | unit   | geo   |   time |   value | downloaded_at              |
|---:|:-----------|:-------|:------|-------:|--------:|:---------------------------|
|  0 | DEPRATIO1  | PC     | AL    |   2014 |    46.1 | 2021-01-15 16:59:59.706976 |
|  1 | DEPRATIO1  | PC     | AL    |   2015 |    45.9 | 2021-01-15 16:59:59.706976 |
|  2 | DEPRATIO1  | PC     | AL    |   2016 |    45.5 | 2021-01-15 16:59:59.706976 |
|  3 | DEPRATIO1  | PC     | AL    |   2017 |    45.5 | 2021-01-15 16:59:59.706976 |
|  4 | DEPRATIO1  | PC     | AL    |   2018 |    45.4 | 2021-01-15 16:59:59.706976 |

This dataset contains 540 166 rows of data.

## Data set 3
Citation: ESS Round 9: European Social Survey Round 9 Data (2018). Data file edition 3.0. NSD - Norwegian Centre for Research Data, Norway – Data Archive and distributor of ESS data for ESS ERIC. doi:10.21338/NSD-ESS9-2018.


Name of DAG: `ESS9`

|    |   idno | region   |   regunit | cntry   |   nwspol |   netusoft |   netustm |   ppltrst |   pplfair |   pplhlp |   polintr |   psppsgva |   actrolga |   psppipla |   cptppola |   trstprl |   trstlgl |   trstplc |   trstplt |   trstprt |   trstep |   trstun |
|---:|-------:|:---------|----------:|:--------|---------:|-----------:|----------:|----------:|----------:|---------:|----------:|-----------:|-----------:|-----------:|-----------:|----------:|----------:|----------:|----------:|----------:|---------:|---------:|
|  0 |     27 | AT22     |         2 | AT      |       60 |          5 |       180 |         2 |         2 |        2 |         3 |          3 |          2 |          2 |          2 |         5 |        10 |        10 |         5 |         5 |        5 |        5 |
|  1 |    137 | AT21     |         2 | AT      |       10 |          5 |        20 |         7 |         8 |        7 |         2 |          3 |          2 |          3 |          2 |         7 |         8 |         8 |         3 |         4 |        5 |        2 |
|  2 |    194 | AT33     |         2 | AT      |       60 |          4 |       180 |         5 |         7 |        7 |         4 |          2 |          1 |          3 |          2 |         6 |         8 |         8 |         5 |         5 |        5 |        5 |
|  3 |    208 | AT21     |         2 | AT      |       45 |          5 |       120 |         3 |         9 |        5 |         3 |          2 |          2 |          3 |          1 |         0 |         5 |         8 |         3 |         3 |        0 |        2 |
|  4 |    220 | AT32     |         2 | AT      |       30 |          1 |       nan |         5 |         8 |        4 |         2 |          1 |          1 |          1 |          3 |         7 |         8 |         8 |         7 |         7 |        5 |        5 |

This dataset contains 49 519 rows of data.

## What if the data was increased by 100x?
Since the project used Redshift, one way to handle a 100-fold increase in data would be to add more clusters, or increase the node types.

The downloading from the API should also be able to handle this, as it is divided into multiple subtasks.

## What if the pipeline were to run every day, at 7 am?
This is already taken care of, since the project uses Airflow.

## What if the database needed to be accessed by 100+ people?
Amazon Redshift should be able to scale in order to handle access from 100 + people.


## How to run
Requirements:
- Docker compose must be installed
- A file called `.env` with the following variables:
    - AIRFLOW__CORE__SQL_ALCHEMY_CONN
    - AIRFLOW__CORE__EXECUTOR
    - AIRFLOW__CORE__FERNET_KEY
    - POSTGRES_USER
    - POSTGRES_PASSWORD
    - POSTGRES_DB
- An AWS S3 bucket, with the bucket name assigned to the Airflow variable `s3_bucket_name`
- An AWS Redshift instance with the credentials stored with the Airflow Conn Id `redshift`
- AWS credentials stored with the Airflow Conn Id `aws_credentials`
- Airflow can be run with the command `docker-compose up`. The DAGs should then be available in the Airflow UI.
