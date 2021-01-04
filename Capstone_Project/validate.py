
def validate_dimensions(spark,dimensions,output_data):
    for dimension in dimensions:
        dimension = spark.read.parquet(output_data+dimension+'.parquet')
        print(type(dimension))