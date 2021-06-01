import pandas as pd
from pyspark.sql import SparkSession
import os
import configparser
import etl_functions
from variables import linebackers, safeties, cornerbacks, output_data
from datetime import datetime
from pyspark.sql.functions import udf, col, monotonically_increasing_id, unix_timestamp, to_date, from_unixtime, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, countDistinct, coalesce
from pyspark.sql.types import DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ["PUSPARK_SUBMIT_ARGS"] = "--driver-memory 4g"

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    return spark


def main():
    spark = create_spark_session()

    full_bowl_roster_df, full_stats_df = etl_functions.create_full_weeks_dataframe(spark)

    # Vamos a necesitar el roster_df para poder filtrar por posiciones a la hora de hacer las dimension tables de estadísticas por jugador
    roster_df = etl_functions.create_roster_dataframe(spark, full_bowl_roster_df)

    # Separadas en dos funciones que llaman a otras, se crean las dimensiones del ataque y la defensa
    deffense_dfs = etl_functions.create_deffense_dimensions(spark, full_stats_df)
    offense_dfs = etl_functions.create_offense_dimensions(spark, full_stats_df)

    #TODO: Uncomment this:
    # etl_functions.data_quality_checks(spark, roster_df, deffense_dfs, offense_dfs)

if __name__ == "__main__":
    main()
