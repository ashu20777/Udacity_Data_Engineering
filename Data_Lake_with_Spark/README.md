# Project: Data Lake

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project we an ETL pipeline that extracts the data from S3, processes them using Spark on an EMR cluster, and load the data back into S3 as a set of dimensional tables in parquet format. 

## How to run
**From local machine:**	
- Populate the `dl.cfg` config file in the root folder with the AWS credentials:
```
KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY
```
- Create an S3 bucket `s3a://udacity-data-lake-w-spark/` to store the output files
- Run the following command:
`python etl.py`


**From EMR Cluster:**	
* Launch an EMR cluster using your AWS account or CLI with the following settings:
	- Release: `emr-6.0.0` or later
	- Applications: `Spark`: Spark on Hadoop YARN 
	- Instance type: `m5.xlarge`
	- Number of instance: `5`
	- Choose an `EC2 key pair` to connect to master node using SSH (Putty for Windows)
	- check the `Use AWS Glue Data Catalog for table metadata` option to run Spark using Jupyter notebook
* Enable SSH connection to the master node by adding a new inbound rule to the security group: Type: `SSH`, Port:`22` Source: `Custom 0.0.0.0/0`
* Connect to the Master node EC2 machine using SSH (putty for Windows)
* Install GIT on master node: `$ sudo yum install make git`
* Clone github repository on master node: `$ git clone https://github.com/ashu20777/Udacity_Data_Engineering/`
* Run the following command: `$ spark-submit --master yarn etl.py`
		
		
## Project structure

The files found at this project are the following:

- dl.cfg: Config file with AWS credentials.
- etl.py: Program that extracts songs and log data from S3, transforms it using Spark, and loads the Fact and Dimension tables back to S3 in parquet format.
- data_lake.ipynb: Jupyter notebook, for running the program on Jupyter instead of the command line. In order to run, import this notebook on the EMR cluster.
- README.md: Current file, contains detailed information about the project.

## ETL pipeline

1. Read data from S3
    - Song data: `s3://udacity-dend/song_data`
    - Log data: `s3://udacity-dend/log_data`

2. Process the data using spark and create the five tables listed below: 
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

3. Load it back to S3

    Write the Fact and Dimension tables to partitioned parquet files in S3.

    Each of the five tables are written to parquet files in an S3 bucket. Each table has its own folder within the bucket. Songs table files are partitioned by year and then artist. Time table files are partitioned by year and month. Songplays table files are partitioned by year and month.

### Source Data
- **Song datasets**: all json files are nested in subdirectories under *s3a://udacity-dend/song_data*. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: all json files are nested in subdirectories under *s3a://udacity-dend/log_data*. A sample of a single row of each files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```