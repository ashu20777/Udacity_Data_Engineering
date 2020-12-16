import configparser
from datetime import datetime
import os
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
	""" 
		Create a Spark Session with hadoop-aws package,
		which is a library used to connect to S3
	"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Read song_data json files from S3 and transfrom it using Spark to create songs and artist tables,
        and then load it back to S3 in parquet format after applying the appropriate partitioning
        
        Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files in S3
            output_data : S3 bucket where Data Warehouse tables will be stored
    """
	# get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs_data")

    # extract columns to create songs table
    songs_table = spark.sql('''
    select distinct song_id, title, artist_id, year, duration
    from songs_data where song_id is not null
    ''')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(output_data + "songs.parquet")

    # extract columns to create artists table
    artists_table = spark.sql('''
    select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    from songs_data where artist_id is not null
    ''')

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data +  "artists.parquet")


def process_log_data(spark, input_data, output_data):
    """
        Read log_data and song_data json files from S3 and transform it using Spark to create users,time, and songplays tables,
        and then load them back to S3 in parquet format after applying the appropriate partitioning
        
        Parameters:
            spark       : Spark Session
            input_data  : location of log_data json files in S3
            output_data : S3 bucket where Data Warehouse tables will be stored        
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("log_data")

    # extract columns for users table    
    users_table = spark.sql('''
    SELECT distinct cast(userid as INT) as user_id, firstname as first_name, lastname as last_name, gender, level 
    from log_data where page = 'NextSong' and userid is not null
    ''')

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data +  "users.parquet")

    # extract columns to create time table
    time_table = spark.sql('''
     SELECT distinct to_timestamp(ts/1000) as start_time
     ,extract(hour from (to_timestamp(ts/1000))) as hour
     ,extract(day from (to_timestamp(ts/1000))) as day
     ,extract(week from (to_timestamp(ts/1000))) as week
     ,extract(month from (to_timestamp(ts/1000))) as month
     ,extract(year from (to_timestamp(ts/1000))) as year
     ,date_format(to_timestamp(ts/1000),'EEEE') as weekday                        
     from log_data where page = 'NextSong' and ts is not null
    ''')

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + "time.parquet")

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    songs_df = spark.read.json(song_data)
    songs_df.createOrReplaceTempView("songs_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
    SELECT  
	to_timestamp(l.ts/1000) as start_time,
    extract(year from (to_timestamp(l.ts/1000))) as start_year,
	extract(month from (to_timestamp(ts/1000))) as start_month,
	cast(l.userid as INT) as user_id, 
	l.level, 
	s.song_id, 
	s.artist_id, 
	l.sessionid, 
	l.location, 
	l.useragent
    from log_data l 
    left join songs_data s on l.song=s.title and l.artist=s.artist_name
    where l.page = 'NextSong'
    ''')

    # add auto-increment ID column
    songplays_table = songplays_table.withColumn("songplay_id", F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("start_year", "start_month").mode('overwrite').parquet(output_data + "songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-lake-w-spark/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
