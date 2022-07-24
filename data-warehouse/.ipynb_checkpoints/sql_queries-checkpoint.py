import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(
    event_id         INTEGER IDENTITY(0,1),
    artist           VARCHAR(MAX),
    auth             VARCHAR(20),
    firstName        VARCHAR(20),
    gender           VARCHAR(3),
    itemInSession    INTEGER,
    lastName         VARCHAR(20),
    length           NUMERIC,
    level            VARCHAR(10),
    location         VARCHAR(MAX),
    method           VARCHAR(10),
    page             VARCHAR(20),
    registration     NUMERIC,
    sessionId        INTEGER,
    song             VARCHAR(MAX),
    status           INTEGER,
    ts               BIGINT,
    userAgent        VARCHAR(MAX),
    userId           INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
    song_id           VARCHAR(20),
    artist_id         VARCHAR(20),
    artist_latitude   NUMERIC,
    artist_location   VARCHAR(MAX),
    artist_longitude  NUMERIC,
    artist_name       VARCHAR(MAX), 
    duration          NUMERIC,
    num_songs         INTEGER,
    title             VARCHAR(MAX),
    year              INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
    songplay_id    INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time     BIGINT SORTKEY NOT NULL,
    user_id        INTEGER,
    level          VARCHAR(10),
    song_id        VARCHAR(20),
    artist_id      VARCHAR(20),
    session_id     INTEGER,
    location       VARCHAR(MAX),
    user_agent     VARCHAR(MAX)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
    user_id    INTEGER PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name  VARCHAR(50) NOT NULL,
    gender     VARCHAR(3),
    level      VARCHAR(10) NOT NULL
) DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
    song_id    VARCHAR(20) PRIMARY KEY,
    title      VARCHAR(MAX) NOT NULL,
    artist_id  VARCHAR(20) NOT NULL,
    year       INTEGER,
    duration   NUMERIC
) DISTSTYLE AUTO;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(
    artist_id    VARCHAR(20) PRIMARY KEY,
    name         VARCHAR(MAX) NOT NULL,
    location     VARCHAR(MAX),
    latitude     NUMERIC,
    longitude    NUMERIC
) DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(
    start_time    BIGINT PRIMARY KEY SORTKEY DISTKEY,
    hour          INTEGER NOT NULL,
    day           INTEGER NOT NULL,
    week          INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    year          INTEGER NOT NULL,
    weekday       INTEGER NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from '{}'
    CREDENTIALS 'aws_iam_role={}'
    JSON '{}'
    REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs from '{}'
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
            se.ts, 
            se.userid, 
            se.level, 
            ss.song_id, 
            ss.artist_id, 
            se.sessionid, 
            se.location, 
            se.useragent
    FROM staging_events se 
    LEFT JOIN staging_songs ss 
    ON ss.title=se.song AND ss.artist_name=se.artist AND ss.duration=se.length
    WHERE se.ts IS NOT NULL;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
            userid, 
            firstname, 
            lastname, 
            gender, 
            level
    FROM staging_events
    WHERE NOT (
        userid IS NULL OR
        firstname IS NULL OR
        lastname IS NULL OR
        level IS NULL
    );
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT 
            song_id, 
            title, 
            artist_id, 
            year, 
            duration
    FROM staging_songs
    WHERE NOT (
        song_id IS NULL OR
        title IS NULL OR
        artist_id IS NULL
    );
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
            artist_id, 
            artist_name, 
            artist_location, 
            artist_latitude, 
            artist_longitude
    FROM staging_songs
    WHERE NOT (
        artist_id IS NULL OR
        artist_name IS NULL
    );
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT 
            ts,
            extract(hour from (timestamp 'epoch' + ts * interval '0.001 second')),
            extract(day FROM (timestamp 'epoch' + ts * interval '0.001 second')),
            extract(week FROM (timestamp 'epoch' + ts * interval '0.001 second')),
            extract(month FROM (timestamp 'epoch' + ts * interval '0.001 second')),
            extract(year FROM (timestamp 'epoch' + ts * interval '0.001 second')),
            extract(weekday FROM (timestamp 'epoch' + ts * interval '0.001 second'))
    FROM staging_events
    WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
