
def clean_airport_codes(spark,df_airports):
    df_airports.createOrReplaceTempView("airports")

    df_airports_clean = spark.sql("""
    select name, iso_country, iso_region, municipality, iata_code, local_code
    from airports where iso_country='US' and local_code is not null
    """)

    print(f"Cleaning Airport Codes... {df_airports_clean.count()} rows returned.")
    return df_airports_clean


def clean_demographics(spark,df_demo):
    df_demo.createOrReplaceTempView("demo")

    df_demo_clean = spark.sql("""
    select distinct city,`State Code` state,cast(`Median Age` as float) median_age, 
    cast(`Male Population` as int) male_population,cast(`Female Population` as int) female_population, 
    cast(`Total Population` as int) total_population, cast(`Foreign-born` as int) foreign_born from demo 
    where state is not null and city is not null
    """)

    print(f"Cleaning Demographics data... {df_demo_clean.count()} rows returned.")
    return df_demo_clean


def clean_immigration_data(spark, df_visits, df_airport_codes, df_countries, df_states, df_visa):
    df_visits.createOrReplaceTempView("visits")
    df_airport_codes.createOrReplaceTempView("airport_codes")
    df_countries.createOrReplaceTempView("countries")
    df_states.createOrReplaceTempView("states")
    df_visa.createOrReplaceTempView("visa")

    df_visits_format = spark.sql("""SELECT cast(cicid as int) id, cast(i94cit as int) country_citizenship, 
    cast(i94res as int) country_residence, i94port port, date_add('1960-01-01', cast(arrdate as int)) arrival_date, 
    date_add('1960-01-01', cast(depdate as int)) departure_date, I94addr state, cast(i94bir as int) age,gender, 
    cast(i94visa as int) visa, airline FROM visits 
    """)

    df_visits_format.createOrReplaceTempView("visits")

    df_visits_clean = spark.sql("""SELECT id, country_citizenship, country_residence, port, arrival_date, 
    departure_date, state, age,gender, visa,airline FROM visits 
    where port in (select airport_code from airport_codes)
    and country_residence in (select country_code from countries)
    and country_citizenship in (select country_code from countries)
    and state in (select code from states)
    and visa in (select id from visa)
    """)

    print(f"Cleaning Immigration data... {df_visits_clean.count()} rows returned.")
    return df_visits_clean
