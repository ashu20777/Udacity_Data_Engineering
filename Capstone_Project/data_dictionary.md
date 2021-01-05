
# Data Dictionary

## DIM_AIRPORTS - Dimension Table
 * name: string - airport name
 * iso_country: string - country name
 * iso_region: string - region 
 * municipality: string - municipality
 * iata_code: string - IATA code
 * local_code: string - Local code
 
## DIM_US_DEMO - Dimension Table
 * city: string - city name
 * state: string - state code
 * median age: float - median age
 * male_population: int - male population
 * female_population: int - female population
 * total_population: int - total population
 * foreign born: int - foreign born population
 
## DIM_STATE - Dimension Table
 * code: string - state code
 * name: string - state name
 
## DIM_COUNTRY - Dimension Table
 * country_code: string - country code
 * country_name: string - country name
 
## DIM_VISA - Dimension Table
 * id: int - visa code
 * visa: string - visa type
 
## FACT_VISITS - Fact Table
 * id: integer -  id
 * country_citizenship: integer - country of citizenship (code)
 * country_residence: integer - country of residence (code)
 * port: string - airport code for port of entry
 * arrival_date: date - Arrival date
 * departure_date: date - Departure Date
 * state: string - state code
 * age: integer - age
 * gender: string - Gender
 * visa: integer - visa code
 * airline: string - airline code
 * arrival_day: integer - arrival day of month
 * arrival_month: integer - arrival month
 * arrival_year: integer - arrival year
