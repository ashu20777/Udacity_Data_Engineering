import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime
import io

def process_song_file(cur, filepath):
    ''' 
    Read song data file and load SONGS dimension table
    Parameters:
        cur (connection.cursor()): cursor instance
        filepath (str): path of the song data file
    '''
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year','duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = (df['artist_id'][0], df['artist_name'][0], df['artist_location'][0], df['artist_latitude'][0], df['artist_longitude'][0])
    cur.execute(artist_table_insert, artist_data)

def clean_csv_value(value):
    ''' 
    cleanse data for use by COPY command
    Parameters:
        value: value to be cleansed
    '''
    
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')
    
def process_log_file(cur, filepath):
    ''' 
    Read log file and load Fact & Dimension tables
    Parameters:
        cur (connection.cursor()): cursor instance
        filepath (str): path of the song data file
    '''
        
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.day_name()]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    dict_df = dict(zip(column_labels,time_data))
    time_df = pd.DataFrame(dict_df)

    csv_file_like_object = io.StringIO()
    for i, row in time_df.iterrows():
        row = map(clean_csv_value, list(row))
        csv_file_like_object.write('|'.join(row) + '\n')
    csv_file_like_object.seek(0)
    # bulk load time staging table 
    cur.copy_from(csv_file_like_object, 'stg_time', sep='|')
    # load time dimension table from staging
    cur.execute(time_table_insert)

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
    # insert songplay records
    for index, row in df.iterrows():
         # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts,unit='ms'), row.userId, row.level, row.sessionId, row.location, row.userAgent, songid, artistid)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    ''' 
    Get all JSON files containing songs/log data,
    and pass them to the relevant functions for processing.
    Parameters:
        cur (connection.cursor()): cursor instance
        conn: connection instance
        filepath (str): path of the JSON data file
        func: name of function to be called for each log file
    '''
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
