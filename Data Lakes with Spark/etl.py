import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Create new spark session for processing files
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song JSON files, extract required columns
    (check songify_schema for more info) and load 
    data into S3 bucket
    
    Arguments:
        - spark: spark session object 
            (created by create_spark_session())
        - input_data: path to S3 bucket containing song JSON files
        - output_data: path to S3 bucket for storing processed files
    """
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    songs_table = songs_table.dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').mode('overwrite').parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select(['artist_id','artist_name','artist_location',\
                               'artist_longitude','artist_latitude'])
    artists_table = artists_table.dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists")


def process_log_data(spark, log_input_data, song_input_data, output_data):
    """
    Process log JSON files, extract required columns
    (check songify_schema for more info) and load 
    data into S3 bucket
    
    Arguments:
        - spark: spark session object 
            (created by create_spark_session())
        - log_input_data: path to S3 bucket containing song JSON files
        - song_input_data: path to S3 bucket containing log JSON files
        - output_data: path to S3 bucket for storing processed files
    """
    # get filepath to log data file
    log_data = log_input_data

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(['userId','firstName','lastName','gender','level'])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000))
    df = df.withColumn('ts', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('start_time', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
        "start_time",
        hour("start_time").alias("hour"),
        dayofmonth("start_time").alias("day"),
        weekofyear("start_time").alias("week"),
        month("start_time").alias("month"),
        year("start_time").alias("year"),
        dayofweek("start_time").alias("weekday")
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').mode('overwrite').parquet(output_data + 'time')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_input_data)
    
    # merge data sets together
    merged_data = df.join(song_df,
                          [df.artist == song_df.artist_name,
                          df.length == song_df.duration,
                          df.song == song_df.title],
                          'inner')
                           #left_on = ['artist','length','song'],
                           #right_on = ['artist_name','duration','title'])
    
    # handle duplicates
    merged_data.dropDuplicates(subset=['start_time'])
    
    # add songplay_id
    merged_data = merged_data.withColumn('songplay_id', monotonically_increasing_id())

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = merged_data.select(['songplay_id','start_time',
                                          'userId','level',
                                          'song_id','artist_id',
                                          'sessionId','location',
                                          'userAgent'])
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table\
        .withColumn('year', year(col('start_time')))\
        .withColumn('month', month(col('start_time')))\
        .write\
        .partitionBy('year','month')\
        .mode("overwrite")\
        .parquet(output_data + 'songplays')
        


def main():
    """
    JSON file processing using Spark to perform ETL
    from S3 bucket into a new S3 bucket 
    """
    spark = create_spark_session()
    song_data = config.get('DATA','SONG_DATA')
    log_data = config.get('DATA','LOG_DATA')
    output_data = config.get('DATA','OUTPUT_DATA')
    
    #process_song_data(spark, song_data, output_data)    
    process_log_data(spark, log_data, song_data, output_data)


if __name__ == "__main__":
    main()
