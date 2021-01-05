
def load_dimensions(output_data, df_countries, df_states, df_visa, df_demo_clean, df_airports_clean):
    """
        Read song_data json files from S3 and transfrom it using Spark to create songs and artist tables,
        and then load it back to S3 in parquet format after applying the appropriate partitioning

        Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files in S3
            output_data : S3 bucket where Data Warehouse tables will be stored
    """
    df_visa.write.mode('overwrite').parquet(output_data + "dim_visa.parquet")
    df_states.write.mode('overwrite').parquet(output_data + "dim_state.parquet")
    df_countries.write.mode('overwrite').parquet(output_data + "dim_country.parquet")
    df_demo_clean.write.mode('overwrite').parquet(output_data + "dim_us_demo.parquet")
    df_airports_clean.write.mode('overwrite').parquet(output_data + "dim_airports.parquet")

    print("Dimension load completed...")


def load_fact(spark,output_data,df_visits_clean):

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
