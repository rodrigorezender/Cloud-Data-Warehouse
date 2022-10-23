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
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist          TEXT,
        auth            TEXT,
        first_name      TEXT,
        gender          TEXT,
        item_in_session INTEGER,
        last_name       TEXT,
        length          FLOAT4,
        level           TEXT,
        location        TEXT,
        method          TEXT,
        page            TEXT,
        registration    FLOAT8,
        session_id      INTEGER,
        song            TEXT,
        status          INTEGER,
        ts              BIGINT,
        user_agent      TEXT,
        user_id         TEXT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs           INTEGER,
        artist_id           TEXT,
        artist_latitude     REAL,
        artist_longitude    REAL,
        artist_location     TEXT,
        artist_name         TEXT,
        song_id             TEXT,
        title               TEXT,
        duration            FLOAT4,
        year                SMALLINT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id    BIGINT IDENTITY(1, 1) PRIMARY KEY,
        start_time     TIMESTAMP NOT NULL,
        user_id        TEXT NOT NULL,
        level          TEXT,
        song_id        TEXT,
        artist_id      TEXT,
        session_id     INTEGER,
        location       TEXT,
        user_agent     TEXT
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id     TEXT PRIMARY KEY,
        first_name  TEXT,
        last_name   TEXT,
        gender      TEXT,
        level       TEXT
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id     TEXT PRIMARY KEY,
        title       TEXT,
        artist_id   TEXT,
        year        SMALLINT,
        duration    FLOAT4
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id   TEXT PRIMARY KEY,
        name        TEXT,
        location    TEXT,
        latitude    FLOAT4,
        longitude   FLOAT4
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time  TIMESTAMP PRIMARY KEY,
        hour        SMALLINT,
        day         SMALLINT,
        week        SMALLINT,
        month       SMALLINT,
        year        SMALLINT DISTKEY,
        weekday     SMALLINT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON {} region '{}';
""").format(
    'staging_events',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
    config['CLUSTER']['DWH_REGION']
)

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON 'auto' region '{}';
""").format(
    'staging_songs',
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['CLUSTER']['DWH_REGION']
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT
        TIMESTAMP 'epoch' + (se.ts/1000 * INTERVAL '1 second'),
        se.user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.session_id,
        se.location,
        se.user_agent
    FROM staging_events se
    JOIN staging_songs ss ON
        se.song = ss.title AND
        se.artist = ss.artist_name
    WHERE
        se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users SELECT DISTINCT (user_id)
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
""")

song_table_insert = ("""
    INSERT INTO songs SELECT DISTINCT (song_id)
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists SELECT DISTINCT (artist_id)
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")


time_table_insert = ("""
    INSERT INTO time
        WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events)
        SELECT DISTINCT
        ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
        FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
