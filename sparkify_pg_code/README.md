# Sparkify PG Code
## Steps
1. The data should be loaded in the 'data' folder
2. Run create_tables.py to create the database and tables
3. Run etl.py to load and transform the data

## Repository Architecture
- create_table.py : execute the sql queries to create the tables and the data model
- etl.py: fill those tables with the data stored inside of ../data/.
    * Each of the input file type (log and song) has a process function
        * Inside of this process function, each of the tables has a process function
    * We have the option to fill those tables either with INSERT or with bulk (COPY) method (parameter bulk)
- sql_queries.py: Store the sql queries
- utils.py: Store the technical functions (connect to the database, prepare the data, bulk load the data)

## Added as a bonus to the project
- Sanitize inputs (see utils.prepare_data)
- Bulk loading (as an option in the process fonctions)