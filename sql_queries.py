import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = ("DROP TABLE IF EXISTS staging_events;")
staging_songs_table_drop = ("DROP TABLE IF EXISTS staging_songs;")
songplay_table_drop = ("DROP TABLE IF EXISTS songplay;")
user_table_drop = ("DROP TABLE IF EXISTS users;")
song_table_drop = ("DROP TABLE IF EXISTS song;")
artist_table_drop = ("DROP TABLE IF EXISTS artist;")
time_table_drop = ("DROP TABLE IF EXISTS time;")

# CREATE TABLES

staging_events_table_create= ("CREATE TABLE IF NOT EXISTS staging_events (artist varchar NULL, \
                                                                          auth varchar NULL, \
                                                                          first_name varchar NULL, \
                                                                          gender varchar NULL, \
                                                                          itemInSession varchar NULL, \
                                                                          lastName varchar NULL, \
                                                                          length varchar NULL, \
                                                                          level varchar NULL, \
                                                                          location varchar NULL, \
                                                                          method varchar NULL, \
                                                                          page varchar NULL, \
                                                                          registration varchar NULL, \
                                                                          sessionId varchar NULL, \
                                                                          song varchar NULL, \
                                                                          status varchar NULL, \
                                                                          ts varchar NULL, \
                                                                          userAgent varchar NULL, \
                                                                          userId varchar NULL);")

staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS staging_songs (num_songs varchar NULL, \
                                                                         artist_id varchar NULL, \
                                                                         artist_latitude varchar NULL, \
                                                                         artist_longitude varchar NULL, \
                                                                         artist_location varchar NULL, \
                                                                         artist_name varchar NULL, \
                                                                         song_id varchar NULL, \
                                                                         title varchar NULL, \
                                                                         duration varchar NULL, \
                                                                         year varchar NULL);")
                                                                         

songplay_table_create = ("CREATE TABLE IF NOT EXISTS FactSongplays (songplay_id int IDENTITY(0,1) PRIMARY KEY, \
                                                                start_time timestamp, \
                                                                user_id varchar references DimUsers(user_id), \
                                                                level varchar, \
                                                                song_id varchar references DimSongs(song_id), \
                                                                artist_id varchar references DimArtists(artist_id), \
                                                                session_id varchar, \
                                                                location varchar, \
                                                                user_agent varchar);")

user_table_create = ("CREATE TABLE IF NOT EXISTS DimUsers (user_id int PRIMARY KEY NOT NULL, \
                                                       first_name varchar, \
                                                       last_name varchar, \
                                                       gender varchar, \
                                                       level varchar);")

song_table_create = ("CREATE TABLE IF NOT EXISTS DimSongs (song_id varchar PRIMARY KEY NOT NULL, \
                                                        title varchar, \
                                                        artist_id varchar, \
                                                        year int, \
                                                        duration float);")

artist_table_create = ("CREATE TABLE IF NOT EXISTS DimArtists (artist_id varchar PRIMARY KEY NOT NULL, \
                                                            name varchar, \
                                                            location varchar, \
                                                            latitude float, \
                                                            longitude float);")

time_table_create = ("CREATE TABLE IF NOT EXISTS DimTime (start_time timestamp NOT NULL, \
                                                       hour int, \
                                                       day int, \
                                                       week int, \
                                                       month int, \
                                                       year int, \
                                                       weekday int);")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
                         CREDENTIALS 'aws_iam_role={}'
                         compupdate off region 'us-west-2'
                         format as json {};
                         """).format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs FROM {}
                         CREDENTIALS 'aws_iam_role={}'
                         compupdate off region 'us-west-2'
                         format as json 'auto';
                         """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))
                         

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO FactSongplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                         SELECT DISTINCT
                                 timestamp 'epoch' + CAST(ev.ts AS BIGINT)/1000 * interval '1 second'AS start_time
                                ,ev.userid AS user_id
                                ,ev.level AS level
                                ,so.song_id AS song_id
                                ,so.artist_id As artist_id
                                ,ev.sessionid AS session_id
                                ,ev.location AS location
                                ,ev.useragent AS user_agent
                        FROM staging_events AS ev
                        JOIN staging_songs AS so ON ev.song = so.title
                                                AND ev.artist = so.artist_name   
                        """)

user_table_insert = ("""INSERT INTO DimUsers (user_id, first_name, last_name, gender, level) 
                        SELECT DISTINCT
                                CAST(userId AS int)
                               ,first_name
                               ,lastname
                               ,gender
                               ,level
                        FROM staging_events
                        WHERE userId != ''
                        """)

song_table_insert = ("""INSERT INTO DimSongs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT 
                                song_id
                               ,title
                               ,artist_id
                               ,CAST(year AS int)
                               ,CAST(duration AS float)
                        FROM staging_songs;

""")

artist_table_insert = ("""INSERT INTO DimArtists(artist_id, name, location, latitude, longitude)
                         SELECT DISTINCT  
                                 artist_id
                                ,artist_name
                                ,artist_location
                                ,CAST(artist_latitude AS float)
                                ,CAST(artist_longitude AS float)
                         FROM staging_songs;
                         """) 
time_table_insert = ("""INSERT INTO DimTime (start_time, hour, day, week, month, year, weekday)
                        SELECT 
                                 start_time
                                ,EXTRACT(HOUR FROM start_time) AS hour
                               ,EXTRACT(DAY FROM start_time) AS day
                               ,EXTRACT(WEEK FROM start_time) AS week
                               ,EXTRACT(MONTH FROM start_time) AS month
                               ,EXTRACT(YEAR FROM start_time) AS year
                               ,EXTRACT(WEEKDAY FROM start_TIME) AS weekday
                        FROM (
                            SELECT 
                                    timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' AS start_time
                            FROM staging_events)
                            """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
