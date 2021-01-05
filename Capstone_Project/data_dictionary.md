
# Data Dictionary

## DIM_AIRPORTS - Dimension Table
 * name: string - airport name
 * iso_country: string - country name
 * iso_region: string - region 
 * municipality: string - municipality
 * iata_code: string - IATA code
 * local_code: string - Local code
 
## U.S. Demographic by State
 * State: string -Full state name
 * state_code: string - State code
 * Total_Population: double - Total population of the state
 * Male_Population: double - Total Male population per state
 * Female_Population: double - Total Female population per state
 * American_Indian_and_Alaska_Native: long - Total American Indian and Alaska Native population per state
 * Asian: long - Total Asian population per state
 * Black_or_African-American: long - Total Black or African-American population per state
 * Hispanic_or_Latino: long - Total Hispanic or Latino population per state 
 * White: long - Total White population per state 
 * Male_Population_Ratio: double - Male population ratio per state
 * Female_Population_Ratio: double - Female population ratio per state
 * American_Indian_and_Alaska_Native_Ratio: double - Black or African-American population ratio per state
 * Asian_Ratio: double - Asian population ratio per state
 * Black_or_African-American_Ratio: double - Black or African-American population ratio per state
 * Hispanic_or_Latino_Ratio: double - Hispanic or Latino population ratio per state 
 * White_Ratio: double - White population ratio per state 
 
## Airlines
 * Airline_ID: integer - Airline id
 * Name: string - Airline name
 * IATA: string - IATA code
 * ICAO: string - ICAO code
 * Callsign: string - name code
 * Country: string - country
 * Active: string - Active

## Countries
 * cod_country: long - Country code
 * country_name: string - Country name
 

## Visas
 * cod_visa: string - visa code
 * visa: string - visa description
 
## Mode to access
 * cod_mode: integer - Mode code
 * mode_name: string - Mode description


# Fact Table (Inmigration Registry)
 * cic_id: integer - CIC id
 * cod_port: string - Airport code
 * cod_state: string - US State code
 * visapost: string - Department of State where where Visa was issued
 * matflag: string - Match flag - Match of arrival and departure records
 * dtaddto: string - Character Date Field - Date to which admitted to U.S. (allowed to stay until)
 * gender: string - Gender
 * airline: string - Airline code
 * admnum: double - Admission Number
 * fltno: string - Flight number of Airline used to arrive in U.S.
 * visatype: string - Class of admission legally admitting the non-immigrant to temporarily stay in U.S
 * cod_visa: integer - Visa code
 * cod_mode: integer - Mode code
 * cod_country_origin: integer - Country of origin code
 * cod_country_cit: integer - City code of origin
 * year: integer - Year
 * month: integer - Month
 * bird_year: integer - Year of Birth
 * age: integer - Age
 * counter: integer - Used for summary statistics
 * arrival_date: date - Arrival date
 * departure_date: date - Departure Date
 * arrival_year: integer - arrival year
 * arrival_month: integer - Arrival month
 * arrival_day: integer - arrival day of month