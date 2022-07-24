import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Method to create spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Method which reads songs data from s3 and transforms this into 
    into song and artist table and store them back in s3
    
    Args:
        spark: spark session
        input_data: input data path of bucket
        output_data: output data path of bucket
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song-data/A/A/*/*.json')

    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = df.select([
        'song_id',
        'title',
        'artist_id',
        'year',
        'duration'
    ]).dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs_table/')

    # extract columns to create artists table
    artists_table = df.select([
        'artist_id',
        'artist_name',
        'artist_location',
        'artist_latitude',
        'artist_longitude'
    ]).dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')


def process_log_data(spark, input_data, output_data):
    '''
    Method which reads logs data from s3 and transforms this into 
    into user, time and songsplay table and store them back in s3
    
    Args:
        spark: spark session
        input_data: input data path of bucket
        output_data: output data path of bucket
    '''
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log-data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.selectExpr(
        'cast(userId as int) userId',
        'firstName',
        'lastName',
        'gender',
        'level'
    ).dropDuplicates(['userId', 'level'])

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table/')

    # create timestamp column from original timestamp column
    df = df.withColumn('timestamp', (col("ts") / 1000).cast("timestamp"))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select(
            col("ts"),
            year(col("datetime")).alias("year"),
            month(col("datetime")).alias("month"),
            dayofmonth(col("datetime")).alias("day_of_month"),
            hour(col("datetime")).alias("hour"),
            weekofyear(col("datetime")).alias("week_of_year"),
    ).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time_table/')

    df.createOrReplaceTempView("logs")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
        SELECT DISTINCT
                logs.timestamp AS start_time,
                logs.userid,
                logs.level,
                songs.song_id,
                songs.artist_id,
                logs.sessionid AS session_id,
                logs.location,
                logs.useragent AS user_agent
        FROM logs LEFT JOIN songs
        ON songs.title=logs.song AND songs.artist_name=logs.artist AND songs.duration=logs.length
        WHERE logs.timestamp IS NOT NULL
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.withColumn('year',year('start_time')).withColumn('month',month('start_time'))\
    .write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-partitioned-tables/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
