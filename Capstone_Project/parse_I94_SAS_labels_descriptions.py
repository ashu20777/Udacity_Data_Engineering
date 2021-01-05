
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DecimalType

file = "data/I94_SAS_Labels_Descriptions.SAS"


def get_airport_codes(spark):
    """
        Parse SAS_labels_descriptions file for valid airport codes
        and load into a Spark DataFrame

        Parameters:
            spark       : Spark Session
    """
    f = open(file, "r")
    lines = f.readlines()
    airports = []
    for line in lines[302:893]:
        line_split = line.replace("'", "").split('=')
        airports.append({"airport_code": line_split[0].strip(), "airport_name": line_split[1].strip()})

    schema = StructType([
        StructField('airport_code', StringType()),
        StructField('airport_name', StringType())
    ])
    df_airport_codes = spark.createDataFrame(airports, schema)
    print(f"Read {df_airport_codes.count()} airport codes.")
    return df_airport_codes


def get_states(spark):
    """
        Parse SAS_labels_descriptions file for valid state codes
        and load into a Spark DataFrame

        Parameters:
            spark       : Spark Session
    """
    f = open(file, "r")
    lines = f.readlines()
    states = []
    for line in lines[981:1035]:
        line_split = line.replace("'", "").split('=')
        states.append({"code": line_split[0].strip(), "name": line_split[1].strip()})

    schema = StructType([
        StructField('code', StringType()),
        StructField('name', StringType())
    ])
    df_states = spark.createDataFrame(states, schema)
    print(f"Read {df_states.count()} states.")
    return df_states


def get_countries(spark):
    """
        Parse SAS_labels_descriptions file for valid country codes
        and load into a Spark DataFrame

        Parameters:
            spark       : Spark Session
    """
    f = open(file, "r")
    lines = f.readlines()
    countries = []
    for line in lines[9:245]:
        line_split = line.replace("'", "").split('=')
        countries.append({"country_code": int(line_split[0].strip()), "country_name": line_split[1].strip()})

    schema = StructType([
        StructField('country_code', IntegerType()),
        StructField('country_name', StringType())
    ])
    df_countries = spark.createDataFrame(countries, schema)
    print(f"Read {df_countries.count()} countries.")
    return df_countries


def get_visa(spark):
    """
        Parse SAS_labels_descriptions file for valid visa codes
        and load into a Spark DataFrame

        Parameters:
            spark       : Spark Session
    """
    f = open(file, "r")
    lines = f.readlines()
    visa = []
    for line in lines[1046:1049]:
        line_split = line.replace("'", "").split('=')
        visa.append({"id": int(line_split[0].strip()), "visa": line_split[1].strip()})

    schema = StructType([
        StructField('id', IntegerType()),
        StructField('visa', StringType())
    ])
    df_visa = spark.createDataFrame(visa, schema)
    print(f"Read {df_visa.count()} visa types.")
    return df_visa