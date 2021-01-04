from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DecimalType
from parse_I94_SAS_labels_descriptions import *
from clean import *

def create_spark_session():
    """
        Create a Spark Session with saurfang:spark-sas7bdat package,
        which is a library for parsing SAS data (sas7bdat) with Spark SQL
    """
    spark = SparkSession.builder. \
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport().getOrCreate()
    return spark


def process_immigration_data(spark, input_data, output_data):
    """
        Read song_data json files from S3 and transfrom it using Spark to create songs and artist tables,
        and then load it back to S3 in parquet format after applying the appropriate partitioning

        Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files in S3
            output_data : S3 bucket where Data Warehouse tables will be stored
    """


def main():
    """
        Read songs and log data from S3,
        Transform it using Spark into Data Warehouse tables, and
        Load them back to S3 in Parquet format
    """
    spark = create_spark_session()
    input_data = "data/"
    output_data = "results/"

    df_visits = spark.read.parquet("data/immigration_data")
    df_demo = spark.read.csv("data/us-cities-demographics.csv", sep=";", header=True)
    df_airports = spark.read.csv("data/airport-codes_csv.csv", header=True)
    df_airport_codes = get_airport_codes(spark)
    df_countries = get_countries(spark)
    df_states = get_states(spark)
    df_visa = get_visa(spark)

    clean_airport_codes(spark,df_airports)
    clean_demographics(spark,df_demo)
    clean_immigration_data(spark, df_visits, df_airport_codes, df_countries, df_states, df_visa)
    # process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
