import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
     artist varchar,
     auth varchar,
     firstname varchar,
     gender varchar,
     itemInSession integer,
     lastname varchar,
     length numeric,
     level varchar,
     location varchar,
     method varchar,
     page varchar,
     registration bigint,
     sessionId bigint,
     song varchar,
     status integer,
     ts bigint,
     userAgent varchar,
     userId integer
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
    num_songs integer,
    artist_id varchar,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration numeric,
    year integer
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id integer IDENTITY(0,1) sortkey,
    start_time timestamp NOT NULL,
    user_id varchar NOT NULL distkey,
    level varchar,
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL,
    session_id varchar,
    location varchar,
    user_agent varchar    
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id varchar NOT NULL sortkey,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id varchar NOT NULL sortkey,
    title varchar,
    artist_id varchar NOT NULL,
    year int,
    duration numeric
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id varchar NOT NULL sortkey,
    name varchar,
    location varchar,
    latitude numeric,
    longitude numeric
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time timestamp sortkey,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday integer
)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as JSON {}
""").format(config['S3']['LOG_DATA'], DWH_ROLE_ARN, config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json 'auto'
""").format(config['S3']['SONG_DATA'], DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT dateadd(millisecond, se.ts % 1000, dateadd(second, ts/1000,'19700101')) AS start_time,
        se.userId AS user_id, 
        se.level, 
        ss.song_id, 
        ss.artist_id, 
        se.sessionId AS session_id, 
        se.location, 
        se.userAgent AS user_agent
FROM staging_events se
JOIN staging_songs ss
    ON se.artist = ss.artist_name
    AND se.length = ss.duration
    AND se.song = ss.title
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstname, lastname, gender, level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, longitude, latitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_longitude, artist_latitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT dateadd(millisecond,ts % 1000, dateadd(second, ts/1000,'19700101')) AS start_time,
EXTRACT(hour from start_time) AS hour, 
EXTRACT(day from start_time) AS day, 
EXTRACT(week from start_time) AS week, 
EXTRACT(month from start_time) AS month, 
EXTRACT(year from start_time) AS year, 
DATEPART(weekday, start_time) AS weekday
FROM staging_events
WHERE page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
