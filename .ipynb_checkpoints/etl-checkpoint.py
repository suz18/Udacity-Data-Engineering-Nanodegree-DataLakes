import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    This function creates a new Spark session.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function loads source song data from S3 bucket, processes and transforms data 
    into songs and artists tables. The two tables are then written back to a S3 bucket.
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy("year","artist_id").parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']) \
    .withColumnRenamed("artist_name", "name") \
    .withColumnRenamed("artist_location", "location") \
    .withColumnRenamed("artist_latitude", "latitude") \
    .withColumnRenamed("artist_longitude", "longitude") \
    .dropDuplicates()
    
    # write artists table to parquet files
    artists_table = artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    This function loads source log data from S3 bucket, processes and transforms data 
    into users, time and songplays tables. The three tables are then written back to a S3 bucket.
    '''
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level']) \
    .withColumnRenamed("userId", "user_id") \
    .withColumnRenamed("firstName", "first_name") \
    .withColumnRenamed("lastName", "last_name") \
    .dropDuplicates()
    
    # write users table to parquet files
    users_table = users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : int(x / 1000.0))
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : str(datetime.fromtimestamp(int(x / 1000))))
    df = df.withColumn("datetime", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year')
    ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy("year","month").parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')


    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json'))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(df, song_df.title == df.song) \
    .select(
        monotonically_increasing_id().alias('songplay_id'),
        col('datetime').alias('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'),
        month('datetime').alias('month'),
        year('datetime').alias('year'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy("year","month").parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')


def main():
    '''
    The main function is to call functions to create a Spark session, process log and
    song data into tables, and save data back into a S3 bucket.
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3n://suzs/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
