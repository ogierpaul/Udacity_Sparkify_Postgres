# Sparkify project with Postgres
-------
*Udacity Project for Data Engineer Nanodegree*

## Repository structure
* data: sample datasets
* sparkify_pg_code: python routines for creating the Sparkify database and Postgres
* sql_queries: sample sql_queries
* tests: basic test cases

## Project Objective
### Purpose
* Transform raw event data from flat files into analytics-ready database

#### Data Modelling
* Use a Star Schema with Facts (Transactional) and Dimensions (Master Data)
    * User axis
    * Song axis
    * Artist axis
    * Time axis
* Note that the schema is not 3NF as the Fact table duplicates information from the song and artist tables

#### Steps
* Create the tables in PostgreSQL (create_tables.py)
* Load the data from the song attributes and fill the song and artist table
* Load the data from the logs and fill the time, user, songplay (fact) table

### Additional content
#### Data cleansing
When preparing the data before loading:
* we call bleach.clean to sanitize the inputs
* We drop all rows where the primary key is null
* We drop all rows with a duplicate primary key

See the function utils.prepare_data for implementation details

* Additionaly, we use the classes sql.Identifier from psycopg2 to prevent SQL injections attack

#### Bulk loading
For each of the method to update a table, two options are available:
* Bulk = False. Will do a standard UPSERT (INSERT... ON CONFLICT (primary key) DO NOTHING..)
* Bulk = True. Will do a COPY FROM. \
 If we need to do an UPSERT using COPY FROM (possible redundancy with the primary key), \
 We COPY the data to a temporary table and then do an INSERT .. ON CONFLICT from this temporary table to the target table

See the function utils.bulk_copy for implementation details of the bulk update.


#### Analytics query
* Using the star schema, we provide a query to find the top 10 most played songs in 2019 in the sample sql_queries provided

