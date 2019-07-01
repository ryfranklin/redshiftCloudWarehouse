# Project 3
Project 3 consists of building dim and fact tables for a cloud data warehouse.  
We will be copying data from s3 buckets and copying data into a redshift cluster.  
Once data is copied an ETL process will be used to model the cloud data warehouse.  

### Link to entity relation diagram:
https://www.lucidchart.com/invitations/accept/f2d9d7cc-3b8b-4873-a465-56504dc278d2


## create_tables.py
create_tables.py file creates all fact and dimension tables used for the project using sql queries from the sql_queries file.  

#### Import Packages 
```bash
import configparser
iport psycopg2
import boto3
from sql_queries import create_table_queries, drop_table_queries
```
##### Drop tables 
* Make sure staging tables are empty before copying from S3 buckets
```bash
def drop_tables(cur, conn):
    for query in drop_tables_queries:
        cur.execute(query)
        conn.commit()
```

##### Create tables
* loop through create_table_queries array from sql_queries.py
```bash
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
```

Staging Tables:
* staging_events
* staging_songs

Fact Tables:
* FactSongplays

Dim Tables:
* DimUsers
* DimSongs
* DimArtists
* DimTime
    
*Primary keys* and *NOT NULL* constraints are used to identify Unique values per table.  
    
### Python script for executing table creation.
```bash
python create_tables.py
```


## etl.py
etl.py file is used to copy data from s3 buckets into staging tables.  Data is then transformed and loaded into dimensionally modeled tabled.  

Make sure to import the following to etl.py:

``` bash
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
```

Scripts to copy contents from S3 bucket and load into staging tables.  
```bash
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
```
function in etl.py to execute copy and load.
```bash
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
```
function transform and dimensionally modeled tables. 
``` bash
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
```

## How to connect to Posgres

connects to posgres and redshift cluster via .cfg file.  
```bash
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
```

### Python script for executing the ETL process.
```bash
python etl.py
```
