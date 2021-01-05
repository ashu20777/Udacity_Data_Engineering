
def load_dimensions(output_data, df_countries, df_states, df_visa, df_demo_clean, df_airports_clean):
    """
        Load Dimension by writing input Data Frames into parquet files

        Parameters:
            output_data         : location where dimension parquet files will be stored
            df_airports_clean   : Spark DataFrame containing Airports data
            df_countries        : Spark DataFrame containing country codes
            df_states           : Spark DataFrame containing state codes
            df_visa             : Spark DataFrame containing visa codes
    """
    df_visa.write.mode('overwrite').parquet(output_data + "dim_visa.parquet")
    df_states.write.mode('overwrite').parquet(output_data + "dim_state.parquet")
    df_countries.write.mode('overwrite').parquet(output_data + "dim_country.parquet")
    df_demo_clean.write.mode('overwrite').parquet(output_data + "dim_us_demo.parquet")
    df_airports_clean.write.mode('overwrite').parquet(output_data + "dim_airports.parquet")

    print("Dimension load completed...")


def load_fact(spark,output_data,df_visits_clean):
    """
        Load Fact table by partitioning and writing input Data Frame into parquet file

        Parameters:
            spark           : Spark Session
            output_data     : location where fact parquet file will be stored
            df_visits_clean : Spark DataFrame containing immigration data
    """
    df_visits_clean.createOrReplaceTempView("visits")

    # add arrival day/moth/year for partitioning
    df_visits_partition = spark.sql("""select visits.*,
    extract(day from arrival_date) arrival_day,
    extract(month from arrival_date) arrival_month,
    extract(year from arrival_date) arrival_year from visits
    """)

    df_visits_partition.write.partitionBy("arrival_year", "arrival_month", "arrival_day").mode('overwrite').parquet(
        output_data + "fact_visits.parquet")

    print("Fact load completed...")
