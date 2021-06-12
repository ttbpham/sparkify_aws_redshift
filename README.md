# SPARKIFY DATA MODELING ON AMAZON REDSHIFT

## Overview

Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. To optimise the queries of data, an ETL pipeline has been built to load json song and log files from S3 into Amazon Redshift Staging tables and then transform data into Star Schema Database which is designed to address the unique needs of large database designed for analytical purpose (OLAP).

## Data Model

**STAGING TABLES**

- staging_events : contains data from music app log files
- staging_songs: contain data from music app song files

**DIMENSION TABLES**

- users - Primary Key: user_id
- songs - Primary Key: song_id
- artists - Primary Key: artist_id
- time - Primary Key: start_time

**FACT TABLE**: songplays

Primary Key: songplay_id which is system generated sequence number <br/>
Foreign Key: user_id, song_id, artist_id, start_time

To enforce the integrity of data, the Foreign Key referential constraints have been implemented on Fact table

## ETL Design

Due to Foreign Key constraints, the ETL processes must be executed in correct order:

- Drop tables: Fact(songplays) -> Dimension (users,songs,artists,time)
- Load tables: Dimension(users, songs, artists, time) -> Fact(songplays)

Due to Primary Key constraints, the lastest record will be inserted if there are duplicates

## Run Instructions

1. Execute the create_tables.py script to create 2 Staging, 4 Dimension and 1 Fact tables.

``` python create_tables.py ```

2. Execute the etl.py script to load data from json files into staging tables and then Star Schema tables.

``` python etl.py ```

3. Run the sqls in Jupiter Notebook to verify data
