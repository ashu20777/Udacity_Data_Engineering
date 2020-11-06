# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"
stg_time_drop = "drop table if exists stg_time"

# CREATE TABLES

songplay_table_create = ("""
create table songplays (
songplay_id SERIAL PRIMARY KEY,
start_time timestamp,
user_id int,
level varchar(100),
song_id varchar(100),
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
user_id int not NULL,
first_name varchar(100) ,
last_name varchar(100),
gender varchar(1),
level varchar(100),
PRIMARY KEY (USER_ID) 
)
""")

song_table_create = ("""
create table songs (
song_id varchar(100) not NULL,
title varchar(100) not NULL,
artist_id varchar(100) not NULL,
year int,
duration NUMERIC(8,5),
PRIMARY KEY (SONG_ID) 
)
""")

artist_table_create = ("""
create table artists (
artist_id varchar(100) not NULL,
name varchar(100) not NULL,
location varchar(100),
latitude NUMERIC(8,5),
longitude NUMERIC(8,5),
PRIMARY KEY (ARTIST_ID) 
)
""")

time_table_create = ("""
create table time (
start_time timestamp not NULL,
hour int not NULL,
day int not NULL,
week int not NULL,
month varchar(10) not NULL,
year int not NULL,
weekday varchar(10) not NULL,
PRIMARY KEY (START_TIME) 
)
""")

stg_time_create = ("""
create table stg_time (
start_time timestamp not NULL,
hour int not NULL,
day int not NULL,
week int not NULL,
month varchar(10) not NULL,
year int not NULL,
weekday varchar(10) not NULL
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO SONGPLAYS(START_TIME, USER_ID, LEVEL, SESSION_ID, LOCATION, USER_AGENT, SONG_ID, ARTIST_ID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(SONGPLAY_ID) DO NOTHING
""")

user_table_insert = ("""
INSERT INTO USERS(USER_ID, FIRST_NAME, LAST_NAME, GENDER, LEVEL) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(USER_ID) DO UPDATE SET level=EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO SONGS(SONG_ID, TITLE, ARTIST_ID, YEAR, DURATION) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(SONG_ID) DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO ARTISTS(ARTIST_ID, NAME, LOCATION, LATITUDE, LONGITUDE) VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT(ARTIST_ID) DO NOTHING
""")

time_table_insert = ("""
INSERT INTO TIME(START_TIME, HOUR, DAY, WEEK, MONTH, YEAR, WEEKDAY) 
SELECT DISTINCT START_TIME, HOUR, DAY, WEEK, MONTH, YEAR, WEEKDAY FROM STG_TIME 
ON CONFLICT(start_time) DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id,s.artist_id FROM songs s, artists a 
where s.artist_id=a.artist_id and s.title = %s and a.name = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, stg_time_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, stg_time_drop]
