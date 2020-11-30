import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
create table stg_events (
artist varchar(200),
auth varchar(100) ,
firstname varchar(100),
gender varchar(1),
lastname varchar(100),
level varchar(100),
location varchar(100),
page varchar(100),
sessionid int,
song varchar(200),
ts bigint,
useragent varchar(200),
userid int
)
""")

staging_songs_table_create = ("""
create table stg_songs (
artist_id varchar(100),
artist_name varchar(200),
artist_location varchar(200),
artist_latitude NUMERIC(8,5),
artist_longitude NUMERIC(8,5),
song_id varchar(100) ,
title varchar(200) ,
year int,
duration NUMERIC(9,5)
)
""")

songplay_table_create = ("""
create table songplays (
songplay_id int IDENTITY(0,1) PRIMARY KEY,
start_time timestamp sortkey,
user_id int,
level varchar(100),
song_id varchar(100) distkey,
artist_id varchar(100),
session_id int,
location varchar(100),
user_agent varchar(200),
FOREIGN KEY (start_time) REFERENCES TIME(start_time),
FOREIGN KEY (user_id) REFERENCES USERS(USER_ID),
FOREIGN KEY (artist_id) REFERENCES ARTISTS(artist_id),
FOREIGN KEY (song_id) REFERENCES SONGS(song_id)
)
""")

user_table_create = ("""
create table users (
user_id int not NULL sortkey,
first_name varchar(100) ,
last_name varchar(100),
gender varchar(1),
level varchar(100),
PRIMARY KEY (USER_ID) 
)
""")

song_table_create = ("""
create table songs (
song_id varchar(100) not NULL distkey,
title varchar(200) not NULL,
artist_id varchar(100) not NULL,
year int,
duration NUMERIC(9,5),
PRIMARY KEY (SONG_ID) 
)
""")

artist_table_create = ("""
create table artists (
artist_id varchar(100) not NULL sortkey,
name varchar(200) not NULL,
location varchar(200),
latitude NUMERIC(8,5),
longitude NUMERIC(8,5),
PRIMARY KEY (ARTIST_ID) 
)
""")

time_table_create = ("""
create table time (
start_time timestamp not NULL sortkey,
hour int not NULL,
day int not NULL,
week int not NULL,
month varchar(10) not NULL,
year int not NULL,
weekday varchar(10) not NULL,
PRIMARY KEY (START_TIME) 
)
""")

# STAGING TABLES

staging_events_copy = ("""
	copy stg_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2' json 'auto ignorecase';
""").format(config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
	copy stg_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2' json 'auto';
""").format(config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  
	timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time,
	e.userid, 
	e.level, 
	s.song_id, 
	s.artist_id, 
	e.sessionid, 
	e.location, 
	e.useragent
from stg_events e 
left join stg_songs s on e.song=s.title and e.artist=s.artist_name
where e.page = 'NextSong'
""")

user_table_insert = ("""
insert into users(user_id, first_name, last_name, gender, level)
SELECT distinct userid, firstname, lastname, gender, level 
from stg_events s where page = 'NextSong' and userid is not null;
""")

song_table_insert = ("""
insert into songs(song_id, title, artist_id, year, duration)
select distinct song_id, title, artist_id, year, duration
from stg_songs where song_id is not null
""")

artist_table_insert = ("""
insert into artists(artist_id, name, location, latitude, longitude)
select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
from stg_songs where artist_id is not null
""")

time_table_insert = ("""
 insert into time(start_time, hour, day, week, month, year, weekday)
 SELECT distinct timestamp 'epoch' + s.ts/1000 * interval '1 second' as start_time
 ,extract(hour from (timestamp 'epoch' + s.ts/1000 * interval '1 second')) as hour
 ,extract(day from (timestamp 'epoch' + s.ts/1000 * interval '1 second')) as day
 ,extract(week from (timestamp 'epoch' + s.ts/1000 * interval '1 second')) as week
 ,extract(month from (timestamp 'epoch' + s.ts/1000 * interval '1 second')) as month
 ,extract(year from (timestamp 'epoch' + s.ts/1000 * interval '1 second')) as year
, to_char(timestamp 'epoch' + s.ts/1000 * interval '1 second', 'DAY') as weekday                        
from stg_events s where page = 'NextSong' and ts is not null
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
