import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
         Create a apache spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
                loads song_data from S3 and processes it by extracting the songs and artist tables
                and then again loaded back to S3
                
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    print('Song_data was loaded successfully')
    # read song data file
    df = spark.read.json(song_data)
    print('Song_data was read successfully')
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    songs_table.createOrReplaceTempView('songs')
    print('Songs table was created successfully')
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year' ,'artist_id' )\
    .parquet(os.path.join(output_data,'songs/songs.parquet'), 'overwrite')
    print('Songs table was loaded to parquet files successfully')
    
    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name',
                              'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()
    
    artists_table.createOrReplaceTempView('artists')
    print('artists table was created successfully')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'artists/artists.parquet'),'overwrite')
    print('artists table was loaded to parquet files successfully')


def process_log_data(spark, input_data, output_data):
    """
                    loads log_data from S3 and processes it by extracting the songs and artist tables
                    and then again loaded back to S3. Also output from previous function is used in by spark.read.json command
        
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    print('log_data was loaded successfully')

    # read log data file
    df = spark.read.json(log_data)
    print('log_data was read successfully')
    # filter by actions for song plays
    print('filtering.*/*/*/*///*/*/*/.*.*.*.*.*.**.')
    df_actions = df.filter(df.page == 'NextSong')\
    .select('ts', 'user_id', 'level', 'song', 'artist', 'session_id', 'location', 'user_agent')
    print('done.*/*/*/*///*/*/*/.*.*.*.*.*.**.')

    # extract columns for users table    
    users_table = df.select('user_id', 'first_name', 'last_name', 'gender', 'level').dropDuplicates()
    
    users_table.createOrReplaceTempView('users')
    print('users table was created successfully')
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,'users/users.parquet'),'overwrite')
    print('users table was loaded to parquet files successfully')

    # create timestamp column from original timestamp column
    print('creating timestamp column.*/*/*/*///*/*/*/.*.*.*.*.*.**.')
    get_timestamp = udf(lambda x:str(int(int(x)/1000)))
    df_actions = df_actions.withColumn('timestamp', get_timestamp(df_actions.ts))
    print('done.*/*/*/*///*/*/*/.*.*.*.*.*.**.')

    # create datetime column from original timestamp column
    print('creating timestamp column.*/*/*/*///*/*/*/.*.*.*.*.*.**.')
    get_datetime = udf(lambda x:str(datetime.fromtimestamp(int(x)/1000)))
    df_actions = df_actions.withColumn('datetime', get_datetime(df_actions.ts))
    print('done.*/*/*/*///*/*/*/.*.*.*.*.*.**.')
    
    # extract columns to create time table
    time_table = df_actions.select('datatime')\
               .withColumn('start_time',df_actions.datetime)\
               .withColumn('hour', hour('datetime'))\
               .withColumn('day', dayofmonth('datetime'))\
               .withColumn('week', weekofyear('datetime'))\
               .withColumn('month', month('datetime'))\
               .withColumn('year', year('datetime'))\
               .withColumn('weekday', dayfoweek('datetime'))\
               .dropDuplicates()
    time_table.createOrReplaceTempView('time')
    print('users table was created successfully')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year' , 'month')\
    .parquet(os.path.join(output_data, 'time/time.parquet'),'overwrite')
    print('artists table was loaded to parquet files successfully')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    print('Song_data was read successfully')

    # extract columns from joined song and log datasets to create songplays table 
    df_actions = df_actions.alias('log_df')
    song_df = song_df.alias('song_df')
    joined_df = actions_df.join(song_df, col('log_df.artist') == col(
        'song_df.artist_name'), 'inner')
    songplays_table = joined_df.select(
        col('log_df.datetime').alias('start_time'),
        col('log_df.userId').alias('user_id'),
        col('log_df.level').alias('level'),
        col('song_df.song_id').alias('song_id'),
        col('song_df.artist_id').alias('artist_id'),
        col('log_df.sessionId').alias('session_id'),
        col('log_df.location').alias('location'), 
        col('log_df.userAgent').alias('user_agent'),
        year('log_df.datetime').alias('year'),
        month('log_df.datetime').alias('month')) \
        .withColumn('songplay_id', monotonically_increasing_id())

    songplays_table.createOrReplaceTempView('songplays') 
    print('songplays table was created successfully')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month')\
    .parquet(os.path.join(output_data,'songplays/songplays.parquet'),'overwrite')
    print('artists table was loaded to parquet files successfully')



def main():
    spark = create_spark_session()
    input_data = ""
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
