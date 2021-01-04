from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DecimalType
from parse_I94_SAS_labels_descriptions import *
from clean import *
from load import *
from validate import *

def create_spark_session():
    """
        Create a Spark Session with saurfang:spark-sas7bdat package,
        which is a library for parsing SAS data (sas7bdat) with Spark SQL
    """
    spark = SparkSession.builder. \
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport().getOrCreate()
    return spark


def main():
    """
        Read songs and log data from S3,
        Transform it using Spark into Data Warehouse tables, and
        Load them back to S3 in Parquet format
    """
    # create a Spark sesson
    spark = create_spark_session()

    # set input & output data locations
    input_data = "data/"
    output_data = "results/"

    # Gather/read the datasets
    df_visits = spark.read.parquet("data/immigration_data")
    df_demo = spark.read.csv("data/us-cities-demographics.csv", sep=";", header=True)
    df_airports = spark.read.csv("data/airport-codes_csv.csv", header=True)
    df_airport_codes = get_airport_codes(spark)
    df_countries = get_countries(spark)
    df_states = get_states(spark)
    df_visa = get_visa(spark)

    # clean the datassets
    df_airports_clean = clean_airport_codes(spark,df_airports)
    df_demo_clean= clean_demographics(spark,df_demo)
    df_visits_clean = clean_immigration_data(spark, df_visits, df_airport_codes, df_countries, df_states, df_visa)

    # load the fact and dimensions in parquet files
    load_dimensions(output_data, df_countries, df_states, df_visa, df_demo_clean, df_airports_clean)
    load_fact(spark,output_data, df_visits_clean)

    '''
    load_dims = {
        'df_visa':'dim_visa',
        'df_states': 'dim_state',
        'df_countries': 'dim_country',
        'df_demo_clean': 'dim_us_demo',
        'df_airports_clean': 'dim_airports'
    }
    '''
    validate_dimensions(spark,['dim_visa','dim_state','dim_country','dim_us_demo','dim_airports'],output_data)


if __name__ == "__main__":
    main()
