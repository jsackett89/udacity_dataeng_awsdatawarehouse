## DATA WAREHOUSING WITH AWS

The goal of this project is to create a data warehouse that Sparkify can use to analyze listening trends of their users. 

## DATABASE DESIGN

A star schema was chosen here for query speed. The tables are as follows: 

FACT TABLE
songplays -- contains information about user song plays over time
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

DIMENSION TABLES
users - users in the app
user_id, first_name, last_name, gender, level

songs - songs in music database
song_id, title, artist_id, year, duration

artists - artists in music database
artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

## PROJECT TEMPLATE

This project contains several files to perform the ETL from S3 to Redshift: 

`sql_queries.py` -- Contains the DROP, CREATE, and INSERT queries for the tables in the database schema above. 

`create_tables.py` -- Contains the code that executes the CREATE TABLEs queries from `sql_queries.py`

`etl.py` -- Contains the code that copies data from S3 to staging, and then from staging to the Redshift DB

`dwh.cfg` -- Config file with redshift credentials (exported for security purposes)

## STEPS FOR RUNNING PROJECT

1. Create IAM Role/User, Redshift cluster, configure network connectivity in AWS Console

2. Add host, DB password, and IAM ARN to `dwh.cfg`

3. Run `python create_tables.py` to create the database schema.

4. Run `python etl.py` to populate the db. 

5. User Query Editor in AWS Console to validate results.

6. Delete cluster and IAM role. 
