import configparser


# Create a config object
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# Retrieve relevant parameters from the config file
DWH_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


# SQL statements for dropping tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays;"
user_table_drop = "DROP TABLE IF EXISTS dim_users;"
song_table_drop = "DROP TABLE IF EXISTS dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"


# SQL statements for creating tables
staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist             VARCHAR(250),
        auth               VARCHAR(12),
        first_name         VARCHAR(50),
        gender             VARCHAR(1),
        item_in_session    INTEGER,
        last_name          VARCHAR(50),
        length             FLOAT,
        level              VARCHAR(4),
        location           VARCHAR(250),
        method             VARCHAR(5),
        page               VARCHAR(10),
        registration       BIGINT,
        session_id         INTEGER,
        song               VARCHAR(250),
        status             INTEGER,
        ts                 TIMESTAMP,
        user_agent         VARCHAR(500),
        user_id            INTEGER
    );
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs          INTEGER,
        artist_id          VARCHAR(20),
        artist_latitude    FLOAT,
        artist_longitude   FLOAT,
        artist_location    VARCHAR(250),
        artist_name        VARCHAR(250),
        song_id            VARCHAR(20),
        title              VARCHAR(250),
        duration           FLOAT,
        year               INTEGER
    );
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS fact_songplays
    (
        songplay_id        INT IDENTITY(1, 1) PRIMARY KEY,
        start_time         TIMESTAMP NOT NULL,
        user_id            INT NOT NULL,
        level              VARCHAR(4),
        song_id            VARCHAR(20) NOT NULL,
        artist_id          VARCHAR(20) NOT NULL,
        session_id         INT,
        location           VARCHAR(250),
        user_agent         VARCHAR(500)
    );
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS dim_users
    (
        user_id            INT PRIMARY KEY,
        first_name         VARCHAR(50) NOT NULL,
        last_name          VARCHAR(50),
        gender             VARCHAR(1),
        level              VARCHAR(4)
    );
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS dim_songs
    (
        song_id            VARCHAR(20) PRIMARY KEY,
        title              VARCHAR(250) NOT NULL,
        artist_id          VARCHAR(20) NOT NULL,
        year               INT,
        duration           FLOAT
    );
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS dim_artists
    (
        artist_id          VARCHAR(20) PRIMARY KEY,
        name               VARCHAR(250) NOT NULL,
        location           VARCHAR(250),
        latitude           FLOAT,
        longitude          FLOAT
    );
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS dim_time
    (
        start_time         TIMESTAMP PRIMARY KEY,
        hour               INT NOT NULL,
        day                INT NOT NULL,
        week               INT NOT NULL,
        month              INT NOT NULL,
        year               INT NOT NULL,
        weekday            INT NOT NULL
    );
"""


# SQL statements for staging tables
staging_events_copy = """
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""".format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = """
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON 'auto';
""".format(SONG_DATA, DWH_ROLE_ARN)


# SQL statements for loading data into fact and dim tables
songplay_table_insert = """
    INSERT INTO fact_songplays (start_time, user_id, level, song_id,
                                artist_id, session_id, location, user_agent)
    SELECT DISTINCT e.ts AS start_time, e.user_id, e.level,
                    s.song_id, s.artist_id, e.session_id,
                    e.location, e.user_agent
    FROM staging_events AS e
    LEFT JOIN staging_songs AS s
    ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong';
"""

user_table_insert = """
    INSERT INTO dim_users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE user_id IS NOT NULL AND page = 'NextSong';
"""

song_table_insert = """
    INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
"""

artist_table_insert = """
    INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name AS name,
                    artist_location AS location,
                    artist_latitude AS latitude,
                    artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
"""

time_table_insert = """
    INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts AS start_time,
                    EXTRACT(HOUR FROM ts) AS hour,
                    EXTRACT(DAY FROM ts) AS day,
                    EXTRACT(WEEK FROM ts) AS week,
                    EXTRACT(MONTH FROM ts) AS month,
                    EXTRACT(YEAR FROM ts) AS year,
                    EXTRACT(DOW FROM ts) AS weekday
    FROM staging_events
    WHERE ts IS NOT NULL AND page = 'NextSong';
"""


# SQL queries for checking the number of records inserted to each table
staging_events_table_count = "SELECT COUNT(*) FROM staging_events;"
staging_songs_table_count = "SELECT COUNT(*) FROM staging_songs;"
songplay_table_count = "SELECT COUNT(*) FROM fact_songplays;"
user_table_count = "SELECT COUNT(*) FROM dim_users;"
song_table_count = "SELECT COUNT(*) FROM dim_songs;"
artist_table_count = "SELECT COUNT(*) FROM dim_artists;"
time_table_count = "SELECT COUNT(*) FROM dim_time;"


# Create a list of tables
TABLES = ['staging_events', 'staging_songs', 'fact_songplays',
          'dim_users', 'dim_songs', 'dim_artists', 'dim_time']

# SQL query for checking table schemas
CHECK_SCHEMA_QUERY = """
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_name IN {}
    ORDER BY table_name
""".format(tuple(TABLES))


# Analytic questions and SQL queries for validating the dimensional model
Q1 = 'What is the most played song of all time?'
Q2 = 'When is the highest usage time of day by hour for songs?'
Q3 = 'Who are the top 3 most popular artists?'
Q4 = 'Which five users listen to songs the most between midnight and 1 AM?'

Q1_query = """
    SELECT dS.title
    FROM fact_songplays AS fS
    JOIN dim_songs AS dS
    ON fS.song_id = dS.song_id
    GROUP BY dS.title
    ORDER BY COUNT(fS.songplay_id) DESC
    LIMIT 1;
"""

Q2_query = """
    SELECT dT.hour
    FROM fact_songplays AS fS
    JOIN dim_time AS dT
    ON fS.start_time = dT.start_time
    GROUP BY dT.hour
    ORDER BY COUNT(fS.songplay_id) DESC
    LIMIT 1;
"""

Q3_query = """
    SELECT dA.name AS artist
    FROM fact_songplays AS fS
    JOIN dim_artists AS dA
    ON fS.artist_id = dA.artist_id
    GROUP BY artist
    ORDER BY COUNT(fs.songplay_id) DESC
    LIMIT 3;
"""

Q4_query = """
    SELECT dU.first_name || ' ' || dU.last_name AS user
    FROM fact_songplays AS fS
    JOIN dim_time AS dT
    ON fS.start_time = dT.start_time
    JOIN dim_users AS dU
    ON fS.user_id = dU.user_id
    WHERE dT.hour = 0
    GROUP BY dU.first_name, dU.last_name
    ORDER BY COUNT(fS.songplay_id) DESC
    LIMIT 5;
"""


# Consolidate queries into either lists or dictionaries
DROP_TABLE_QUERIES = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]

CREATE_TABLE_QUERIES = [staging_events_table_create,
                        staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create,
                        artist_table_create, time_table_create]

COPY_TABLE_QUERIES = [staging_events_copy, staging_songs_copy]

INSERT_TABLE_QUERIES = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert,
                        time_table_insert]

COUNT_ROWS_QUERIES = [staging_events_table_count, staging_songs_table_count,
                    songplay_table_count, user_table_count, song_table_count,
                    artist_table_count, time_table_count]

ANALYTIC_QUERIES = {
    Q1: Q1_query,
    Q2: Q2_query,
    Q3: Q3_query,
    Q4: Q4_query,
}
