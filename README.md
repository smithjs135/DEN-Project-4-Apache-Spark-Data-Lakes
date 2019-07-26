
# **Project Data Lake-** Song Play Analysis

## Introduction
Sparkify's music streaming application is rapidly growing it's user base.  Their data warehouse that is hosted data on Amazon Web Services is to be upgraded to a data lake.  The analytics team provided JSON logs which track user activity and JSON metadata which contain data related the songs that are currently available on the application.  These files are stored on an S3 bucket. 

## AWS schema design
The JSON metadata is currently stored on S3  from the JSON. An ELT pipeline will be created that extracts song data and loads them back into S3 as a set of Fact and dimension tables.
An S3 bucket is utilized for data storage in JSON format. <br>
The JSONPATH to the log files is: LOG_JSONPATH='s3://udacity-dend/log_json_path.json'<br>

LOG_DATA and SONG_DATA JSON files are contained in the S3 bucket. Information related to the artist and songs is contained in **SONG_DATA**.  Information related to users, their location and listening history, etc.. is contained  **LOG_DATA**.  


## Tables

**Staging tables:** staging_events, staging_events<br> Contain seeded data that is extracted from the JSON files

**Fact table:** Songplays is the only fact table contains reference links Dimension tables 

**Dimension tables:** 
- users: Sparkify users. Songplay reference key is 'user_id'
- songs: Details related songs in the app. Songplay reference key is 'song_id'
- artists: Artists related details. Songplay reference key is 'artist_id'
- time: Timestamp broken down to hour, day, week, month, year, week day. Songplay reference key is 'timestamp' 

## ELT pipeline
- Spark session using: "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
- Song_data stored in JSON is read into a data frame and then processed
	- data related to songs is extracted to song_table and is then written to a parquet file called "songs" that is stored in: "s3a://dend-bucket-js"
	- data related to the artist is extracted to the artist_table and is then written to a parquet file called "artists" that is stored in: "s3a://dend-bucket-js"

- Log_data stored in JSON is read into a data frame and then processed	
	- data related to users is extracted to users_table and is then written to a parquet file called "users" that is stored in: "s3a://dend-bucket-js"
	- data related to time is extracted to the time_table and is then written to a parquet file called "time" that is stored in: "s3a://dend-bucket-js"
	- data related to the fact table is extracted to the songplay_table and is then written to a parquet file called "songplay" that is stored in: "s3a://dend-bucket-js"

## Files


**Song and log data storage locations:**
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

**dwh.cfg** contains credentials required for access to S3 which are considered sensitive information

**etl.py** contents:
- Makes method calls to ELT and data transformation and normalization

**Libraries used:**
- pyspark.sql import SparkSession
- datetime  
- pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
- pyspark.sql import SparkSession
- pyspark.sql.functions import udf, col, desc, to_timestamp, monotonically_increasing_id
- pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


## Running
1. Create an IAM user and retain the Key and Secret Key (sensitive)
2. Initiate the ELT process by running command:
    python etl.py
