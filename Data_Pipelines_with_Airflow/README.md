# Project: Data Pipelines with Airflow

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

In this project we build a high grade data pipeline that is dynamic and built from reusable tasks, can be monitored, and allows easy backfills.


## Source Data
* Song data: `s3://udacity-dend/song_data`
	
	Contains JSON files nested subdirectories. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

* Log data: `s3://udacity-dend/log_data`
		
	Contains JSON files nested subdirectories. A sample of this files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```

## Data Warehouse design

Amazon Redshift is used for storing the Data Warehouse tables.

#### Fact Table
 **songplays**  - records in log data associated with song plays i.e. records with page  `NextSong`
-   _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

#### Dimension Tables
 **users**  - users in the app
	Fields -   _user_id, first_name, last_name, gender, level_
	
 **songs**  - songs in music database
Fields - _song_id, title, artist_id, year, duration_

**artists**  - artists in music database
Fields -   _artist_id, name, location, lattitude, longitude_

  **time**  - timestamps of records in  **songplays**  broken down into specific units
Fields -   _start_time, hour, day, week, month, year, weekday_



## Project structure

* dags
	- `pipleline.py` - The main Airflow DAG for the data pipeline
* plugins
	- operators
		* `stage_redshift.py` - Airflow custom operator to read JSON files from S3 to Redshift
		* `load_fact.py` - Airflow custom operator to load the fact table in Redshift
		* `load_dimension.py` - Airflow custom operator to load dimension tables in Redshift
		* `data_quality.py` - Airflow custom operator for checking data quality
	- helpers
		* `create_tables.sql` - Contains DDL statements to create tables in Redshift
		* `sql_queries.py` - Redshift SQL queries used in the pipeline		