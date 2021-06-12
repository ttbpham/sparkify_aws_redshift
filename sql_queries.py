import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# LOAD CONFIG TO VARIABLES
LOG_DATA = config.get("S3", "LOG_DATA")
ARN = config.get("IAM_ROLE", "ARN")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INTEGER NOT NULL,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR NOT NULL,
    location VARCHAR,
    method VARCHAR NOT NULL,
    page VARCHAR NOT NULL,
    registration FLOAT,
    sessionID INTEGER  NOT NULL,
    song VARCHAR,
    status SMALLINT  NOT NULL,
    ts  BIGINT  NOT NULL,
    userAgent VARCHAR,
    userId  VARCHAR  NOT NULL
)
""")
    
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    artist_id VARCHAR NOT NULL,
    artist_latitude FLOAT,
    artist_location VARCHAR,
    artist_longitude FLOAT,
    artist_name VARCHAR NOT NULL,
    duration FLOAT NOT NULL,
    num_songs INTEGER NOT NULL,
    song_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    year SMALLINT NOT NULL
    )
""")
        
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
     songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
     start_time TIMESTAMP NOT NULL,
     user_id VARCHAR NOT NULL,
     level VARCHAR,
     song_id VARCHAR distkey,
     artist_id VARCHAR sortkey,
     sesson_id INTEGER,
     location VARCHAR,
     user_agent VARCHAR,
     FOREIGN KEY (start_time)
          REFERENCES time (start_time), 
     FOREIGN KEY (user_id)
          REFERENCES users (user_id),
     FOREIGN KEY (song_id)
          REFERENCES songs (song_id),
     FOREIGN KEY (artist_id)
          REFERENCES artists (artist_id)
 )
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR PRIMARY KEY sortkey,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR)
    diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY sortkey distkey,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year SMALLINT NOT NULL,
    duration FLOAT NOT NULL)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY sortkey,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT)
    diststyle auto;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY sortkey,
    hour VARCHAR NOT NULL,
    day VARCHAR NOT NULL,
    week VARCHAR NOT NULL,
    month VARCHAR NOT NULL,
    year VARCHAR NOT NULL,
    weekday VARCHAR NOT NULL)
    diststyle auto;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as JSON {};
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json 'auto';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, sesson_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, userId, level, song, artist, sessionId, location, userAgent    
    FROM staging_events
""")

# insert unique users using the latest level value order by timestamp
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT userId, firstName, lastName, gender, LAST_VALUE(level) OVER (PARTITION BY userId ORDER BY ts DESC rows between unbounded preceding and unbounded following) as level
    FROM staging_events 
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs
""")

# insert unique artists using the latest location order by year
artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, 
    LAST_VALUE(artist_location) OVER (PARTITION BY artist_id ORDER BY year DESC rows between unbounded preceding and unbounded following) as artist_location, 
    artist_latitude, artist_longitude
    FROM staging_songs
""")

# insert distinct Date
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
    EXTRACT(HOUR FROM start_time) as hour,
    EXTRACT(DAY FROM start_time) as day,
    EXTRACT(WEEK FROM start_time) as week,
    EXTRACT(MONTH FROM start_time) as month,
    EXTRACT(YEAR FROM start_time) as year,
    EXTRACT(DOW FROM start_time) as weekday
    FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
