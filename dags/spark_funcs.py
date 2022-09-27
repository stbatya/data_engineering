from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id
import os
import logging

def create_spark_session(master_address):
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.12.129")\
        .appName("myapp")\
        .master("spark://{}".format(master_address))\
        .getOrCreate()
    access_id = os.environ('SPARK_ACCESS_ID')
    access_key = os.environ('SPARK_ACCES_KEY')
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", access_id)
    hadoop_conf.set("fs.s3a.secret.key", access_key)
    return spark

def process_song_data(spark, input_data, output_data):
    """ Process song data and create songs and artists table

        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """
    logging.info('debug0')
    logging.info('debug1')
    song_data = os.path.join(input_data + 'song_data/*/*/*/*.json')

    df = spark.read.json(song_data)
    logging.info('debug2')

    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()

    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id')\
               .parquet(path=output_data + 'songs')

    artists_table = df.select('artist_id', 'artist_name', 'artist_location',\
                              'artist_latitude', 'artist_longitude').distinct()

    artists_table.write.mode("overwrite").parquet(path=output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """ Process log data and create users, time and songplays table
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """
    
    log_data = os.path.join(input_data + 'log_data/*/*/*.json')

    df = spark.read.json(log_data)

    df = df.where(df['page'] == 'NextSong')

    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()

    users_table.write.mode("overwrite").parquet(path=output_data + 'users')

    """
    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df =
    # create datetime column from original timestamp column
    get_datetime = udf()
    df =
    """

    df = df.withColumn('start_time', (df['ts']/1000).cast('timestamp'))
    df = df.withColumn('weekday', date_format(df['start_time'], 'E'))
    df = df.withColumn('year', year(df['start_time']))
    df = df.withColumn('month', month(df['start_time']))
    df = df.withColumn('week', weekofyear(df['start_time']))
    df = df.withColumn('day', dayofmonth(df['start_time']))
    df = df.withColumn('hour', hour(df['start_time']))
    time_table = df.select('start_time', 'weekday', 'year', 'month',\
                           'week', 'day', 'hour').distinct()

    time_table.write.mode('overwrite').partitionBy('year', 'month')\
              .parquet(path=output_data + 'time')


    song_data = os.path.join(input_data + 'song_data/*/*/*/*.json')

    song_df = spark.read.json(song_data)

    logging.info('debug2')
    songplays_table = df.join(song_df, (df.song == song_df.title)\
                                       & (df.artist == song_df.artist_name)\
                                       & (df.length == song_df.duration), "inner")\
                        .distinct()\
                        .select('start_time', 'userId', 'level', 'song_id',\
                                'artist_id', 'sessionId','location','userAgent',\
                                df['year'].alias('year'), df['month'].alias('month'))\
                        .withColumn("songplay_id", monotonically_increasing_id())


    songplays_table.write.mode("overwrite").partitionBy('year', 'month')\
                   .parquet(path=output_data + 'songplays')
