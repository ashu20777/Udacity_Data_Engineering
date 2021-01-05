
def validate_dimensions(spark,dimensions,output_data):
    """
        Run row count validation check on Dimension parquet files

        Parameters:
            spark       : Spark Session
            output_data : location of fact & dimension parquet files
            dimensions  : list containing Dimension names
    """
    for dimension in dimensions:
        df = spark.read.parquet(output_data+dimension+'.parquet')
        df.createOrReplaceTempView(dimension)

        count_rows_df = spark.sql(f"""
        select count(*) as count_rows from {dimension}
        """)
        row_count = int(count_rows_df.first()["count_rows"])
        if row_count != 0:
            print(f"Validation check for {dimension} passed with {row_count} rows")
        else:
            print(f"Validation check for {dimension} failed with {row_count} rows")


def validate_fact(spark,fact,output_data):
    """
        Run data quality checks on Fact table:
        row count check, Unique key check, Missing values check

        Parameters:
            spark       : Spark Session
            output_data : location of fact & dimension parquet files
            fact        : name of fact table
    """
    df = spark.read.parquet(output_data + fact + '.parquet')
    df.createOrReplaceTempView(fact)

    # row count check
    count_rows_df = spark.sql(f"""
    select count(*) as count_rows from {fact}
    """)
    row_count = int(count_rows_df.first()["count_rows"])
    if row_count != 0:
        print(f"Row count check for {fact} passed with {row_count} rows")
    else:
        print(f"Row count check for {fact} failed with {row_count} rows")

    # Uniqueness check
    duplicates_df = spark.sql(f"""
    select id as duplicate_id from {fact} group by id having count(*)>1
    """)
    duplicates = duplicates_df.count()
    if duplicates == 0:
        print(f"Uniqueness check for {fact} passed with {duplicates} duplicate IDs")
    else:
        print(f"Uniqueness check for {fact} failed with {duplicates} duplicate IDs")

    # NULL check
    nulls_df = spark.sql(f"""
    select count(*) as null_rows from {fact}
    where id is null or country_citizenship is null or country_residence is null
    or visa is null or arrival_date is null or state is null or port is null
    """)
    null_rows = int(nulls_df.first()["null_rows"])
    if null_rows == 0:
        print(f"Missing values check for {fact} passed with {null_rows} missing rows")
    else:
        print(f"Missing values check for {fact} failed with {null_rows} missing rows")
