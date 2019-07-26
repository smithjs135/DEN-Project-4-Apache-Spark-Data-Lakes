from pyspark.sql import SparkSession
import os
import configparser
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

print("Config")

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data =  input_data + "song_data/*/*/*/*.json"
    # song_data =  input_data + "song_data/A/A/A/*.json"
    
    print("Reading song file")
    # read song data file
    df = spark.read.json(song_data)
    
    print("Fin Reading song_data json")

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration") 
    
    # write songs table to parquet files partitioned by year and artist
    #songs_table.write.partitionBy("year","artist_id").parquet(output_data + "songs").mode('overwrite')
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(output_data + "songs")
    print("songs_table successfully written to s3 ")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name","artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists")
    print("**artists_table successfully written to s3 **")
    # sourced: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*.json'
    
    # read log data file
    df = spark.read.json(log_data)
    print("finished reading log_data.json")
    
    # filter by actions for song plays
    df = df.filter(col("page") == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level")
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users")
    print("**users_table successfully written to s3 **")
    
    # set time format
    time_format = "yyy-MM-dd HH:MM::ss z"
	# source: https://github.com/FedericoSerini/DEND-Project-4-Apache-Spark-And-Data-Lake/blob/master/etl.py
    
    # create timestamp column from original timestamp column
    df = df.withColumn('ds', to_timestamp(date_format((df.ts / 1000).cast(dataType=TimestampType()), time_format), time_format)) 
    # source https://github.com/FedericoSerini/DEND-Project-4-Apache-Spark-And-Data-Lake/blob/master/etl.py
	
    # create time_table
    time_table = df.select(
        col('ds').alias('start_time'),
        hour('ds').alias('hour'),
        dayofmonth('ds').alias('day'),
        weekofyear('ds').alias('week'),
        month('ds').alias('month'),
        year('ds').alias('year'),
        date_format('ds', 'F').alias('weekday')
    )     
     
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "time")
    print("**time_table successfully written to s3 **")
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs")
    
    # join df and song_df
    df = df.join(song_df, song_df.title == df.song)
    print("successfully joined song_df to df")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select(
        col('ds').alias('start_time'), 
        'userId', 
        'level', 
        'song_id', 
        'artist_id',
        'sessionId',
        'location', 
        'userAgent', 
        'year',
        month('ds').alias('month')
        )
    
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "songplays")
    print("**songplays_table successfully written to s3 **")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://jasonb/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    
if __name__ == "__main__":
    main()
