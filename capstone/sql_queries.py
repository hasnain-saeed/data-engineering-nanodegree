import configparser


#### CONFIG
config = configparser.ConfigParser()
config.read('capstone.cfg')

#### Drop staging table queries
drop_staging_immigration = "DROP TABLE IF EXISTS public.staging_immigration"
drop_staging_countries_table = "DROP TABLE IF EXISTS public.staging_countries"
drop_staging_ports_table = "DROP TABLE IF EXISTS public.staging_ports"
drop_staging_airports_table = "DROP TABLE IF EXISTS public.staging_airports"
drop_staging_demographics_table = "DROP TABLE IF EXISTS public.staging_demographics"
drop_staging_city_temps_table = "DROP TABLE IF EXISTS public.staging_city_temperatures"

#### Drop fact and dim table queries
drop_fact_immigration_table = "DROP TABLE IF EXISTS public.fact_immigration CASCADE;"
drop_dim_countries_table = "DROP TABLE IF EXISTS public.dim_countries CASCADE;"
drop_dim_ports_table = "DROP TABLE IF EXISTS public.dim_ports CASCADE;"
drop_dim_airports_table = "DROP TABLE IF EXISTS public.dim_airports CASCADE;"
drop_dim_demographics_table = "DROP TABLE IF EXISTS public.dim_demographics CASCADE;"
drop_dim_time_table = "DROP TABLE IF EXISTS public.dim_time CASCADE;"


#### Create staging tables
create_staging_immigration_table = """
CREATE TABLE public.staging_immigration
(
    age               DOUBLE precision,
    country_code      TEXT,
    port_code         TEXT,
    mode_of_travel    TEXT,
    reason_for_travel TEXT,
    visatype          TEXT,
    gender            TEXT,
    birth_year        DOUBLE precision,
    arrdate           DOUBLE precision,
    arrival_date      DATE,
    depdate           DOUBLE precision,
    departure_date    DATE
);
"""

create_staging_countries_table = """
CREATE TABLE public.staging_countries (
    country_code    VARCHAR(3) NOT NULL,
    country         VARCHAR(256) NOT NULL
);
"""

create_staging_ports_table = """
CREATE TABLE public.staging_ports (
    port_code    VARCHAR(3),
    city         VARCHAR(256),
    state        VARCHAR(50)
);
"""

create_staging_airports_table = """
CREATE TABLE public.staging_airports (
    ident           VARCHAR(256),
    type            VARCHAR(256),
    name            VARCHAR(256),
    elevation_ft    INTEGER,
    continent       VARCHAR(256),
    iso_country     VARCHAR(256),
    iso_region      VARCHAR(256),
    municipality    VARCHAR(256),
    gps_code        VARCHAR(256),
    iata_code       VARCHAR(256),
    local_code      VARCHAR(256),
    coordinates     VARCHAR(256)
);
"""

create_staging_demographics_table = """
CREATE TABLE public.staging_demographics (
    City                     VARCHAR(256),
    State                    VARCHAR(100),
    "Median Age"             NUMERIC(18,2),
    "Male Population"        INTEGER,
    "Female Population"      INTEGER,
    "Total Population"       BIGINT,
    "Number of Veterans"     INTEGER,
    "Foreign-born"           INTEGER,
    "Average Household Size" NUMERIC(18,2),
    "State Code"             VARCHAR(50),
    Race                     VARCHAR(100),
    Count                    INTEGER
);
"""

create_staging_city_temps_table = """
CREATE TABLE public.staging_city_temperatures
(
    dt                              TIMESTAMP without time zone,
    "AverageTemperature"            DOUBLE precision,
    "AverageTemperatureUncertainty" DOUBLE precision,
    "City"                          TEXT,
    "Country"                       TEXT,
    "Latitude"                      TEXT,
    "Longitude"                     TEXT,
    rank                            INTEGER
)
"""

#### Create data model star schema fact and dimension tables
create_dim_countries_table = """
CREATE TABLE public.dim_countries
(
    country_id          BIGINT PRIMARY KEY,
    country_code        VARCHAR(3) NOT NULL UNIQUE,
    country             VARCHAR(256) NOT NULL UNIQUE,
    average_temperature NUMERIC(16,3)
);
"""

create_dim_ports_table = """
CREATE TABLE public.dim_ports
(
    port_id             BIGINT PRIMARY KEY,
    port_code           VARCHAR(3) UNIQUE,
    port_city           VARCHAR(256),
    port_state          VARCHAR(50),
    average_temperature NUMERIC(16,3)
);
"""

create_dim_airports_table = """
CREATE TABLE public.dim_airports
(
    airport_id      BIGINT PRIMARY KEY,
    port_id BIGINT  UNIQUE,
    airport_type    VARCHAR(256),
    airport_name    VARCHAR(256),
    elevation_ft    INTEGER,
    municipality    VARCHAR(256),
    gps_code        VARCHAR(256),
    iata_code       VARCHAR(256),
    local_code      VARCHAR(256),
    coordinates     VARCHAR(256),
    CONSTRAINT fk_port FOREIGN KEY(port_id) REFERENCES dim_ports(port_id)
);
"""

create_dim_demographics_table = """
CREATE TABLE public.dim_demographics
(
    demographics_id     BIGINT PRIMARY KEY,
    port_id             BIGINT,
    median_age          NUMERIC(18,2),
    male_population     INTEGER,
    female_population   INTEGER,
    total_population    BIGINT,
    number_of_veterans  INTEGER,
    foreign_born        INTEGER,
    avg_household_size  NUMERIC(18,2),
    race                VARCHAR(100),
    demo_count          INTEGER,
    UNIQUE (port_id, race),
    CONSTRAINT fk_port FOREIGN KEY(port_id) REFERENCES dim_ports(port_id)
);
"""

create_dim_time_table = """
CREATE TABLE public.dim_time
(
    sas_timestamp    INTEGER PRIMARY KEY,
    year             INTEGER NOT NULL,
    month            INTEGER NOT NULL,
    day              INTEGER NOT NULL,
    week             INTEGER NOT NULL,
    day_of_week      INTEGER NOT NULL,
    quarter          INTEGER NOT NULL,
);
"""

create_fact_immigration_table = """
CREATE TABLE public.fact_immigration
(
    immigration_id     BIGINT PRIMARY KEY,
    country_id         BIGINT,
    port_id            BIGINT,
    age                INTEGER,
    travel_mode        VARCHAR(100),
    visa_category      VARCHAR(100),
    visa_type          VARCHAR(100),
    gender             VARCHAR(10),
    birth_year         INTEGER,
    arrdate            INTEGER NOT NULL,
    depdate            INTEGER,
    CONSTRAINT fk_port FOREIGN KEY(port_id) REFERENCES dim_ports(port_id),
    CONSTRAINT fk_country FOREIGN KEY(country_id) REFERENCES dim_countries(country_id),
    CONSTRAINT fk_arrdate FOREIGN KEY(arrdate) REFERENCES dim_time(sas_timestamp),
    CONSTRAINT fk_depdate FOREIGN KEY(depdate) REFERENCES dim_time(sas_timestamp)
);
"""

#### Load data from data lake to staging tables

staging_countries_copy = ("""
    COPY public.staging_countries from '{}'
    IAM_ROLE '{}'
    REGION 'us-west-2'
    CSV DELIMITER ','
    IGNOREHEADER 1;
""").format(f"{config['S3']['DATA_LAKE_PATH']}/countries.csv", config['IAM_ROLE']['ARN'])

staging_ports_copy = ("""
    COPY public.staging_ports from '{}'
    IAM_ROLE '{}'
    REGION 'us-west-2'
    CSV DELIMITER ','
    IGNOREHEADER 1;
""").format(f"{config['S3']['DATA_LAKE_PATH']}/ports.csv", config['IAM_ROLE']['ARN'])

staging_airports_copy = ("""
    COPY public.staging_airports from '{}'
    IAM_ROLE '{}'
    REGION 'us-west-2'
    CSV DELIMITER ','
    IGNOREHEADER 1;
""").format(f"{config['S3']['DATA_LAKE_PATH']}/airport-codes_csv.csv", config['IAM_ROLE']['ARN'])

staging_demographics_copy = ("""
    COPY public.staging_demographics from '{}'
    IAM_ROLE '{}'
    REGION 'us-west-2'
    CSV DELIMITER ';'
    IGNOREHEADER 1;
""").format(f"{config['S3']['DATA_LAKE_PATH']}/us-cities-demographics.csv", config['IAM_ROLE']['ARN'])

# copy data from parquet files from s3 to staging tables

# load immigration parquet
staging_immigration_copy = ("""
    COPY public.staging_immigration from '{}'
    IAM_ROLE '{}'
    REGION 'us-west-2'
    FORMAT AS PARQUET;
""").format(f"{config['S3']['DATA_LAKE_PATH']}/immigration_data/year=2016/month=1/arrival_day=1/", config['IAM_ROLE']['ARN'])

# load temperature parquet
staging_city_temperatures_copy = ("""
    COPY public.staging_city_temperatures from '{}'
    IAM_ROLE '{}'
    REGION 'us-west-2'
    FORMAT AS PARQUET;
""").format(f"{config['S3']['DATA_LAKE_PATH']}/us_city_temperature_data/", config['IAM_ROLE']['ARN'])


#### Insert data into fact and dimension tables from staging tables

insert_dim_countries = """
INSERT INTO public.dim_countries (country_code, country, average_temperature)
SELECT t1.* FROM
    (SELECT DISTINCT c.country_code, c.country, t."AverageTemperature"
    FROM public.staging_immigration i
    INNER JOIN public.staging_countries c ON i.country_code = c.country_code
    LEFT JOIN public.staging_country_temperatures t ON UPPER(c.country) =   UPPER(t."Country")
ORDER BY c.country) t1
LEFT JOIN public.dim_countries t2 ON t1.country=t2.country
WHERE t2.country IS NULL
"""

insert_dim_ports = """
INSERT INTO public.dim_ports (port_code, port_city, port_state, average_temperature)
SELECT t1.* FROM
    (SELECT DISTINCT p.port_code, p.city, p.state, t."AverageTemperature"
    FROM public.staging_immigration i
    INNER JOIN public.staging_ports p ON i.port_code = p.port_code
    LEFT JOIN public.staging_city_temperatures t ON UPPER(p.city) = UPPER(t."City")
ORDER BY p.port_code) t1
LEFT JOIN public.dim_ports t2 ON t1.port_code = t2.port_code
WHERE t2.port_code IS NULL
"""

insert_dim_airports = """
INSERT INTO public.dim_airports (port_id, airport_type, airport_name, elevation_ft, municipality, gps_code,
iata_code, local_code, coordinates)
SELECT t1.* FROM
    (SELECT p.port_id, a.type, a.name, a.elevation_ft, a.municipality,
    a.gps_code, a.iata_code, a.local_code, a.coordinates
    FROM public.staging_airports a
    INNER JOIN public.dim_ports p ON a.ident = p.port_code
ORDER BY p.port_code) t1
LEFT JOIN public.dim_airports t2 ON t1.port_id=t2.port_id
WHERE t2.port_id IS NULL
"""

insert_dim_demographics = """
INSERT INTO public.dim_demographics (port_id, median_age, male_population, 
female_population, total_population, number_of_veterans,foreign_born,avg_household_size, race, demo_count)
SELECT t1.* FROM
    (SELECT DISTINCT p.port_id, d."Median Age",d."Male Population",d."Female Population",d."Total Population",
    d."Number of Veterans", d."Foreign-born",d."Average Household Size",d.race, d."count"
    FROM public.dim_ports p
    INNER JOIN public.staging_demographics d 
    ON UPPER(p.port_city) = UPPER(d.city) AND UPPER(p.port_state) = UPPER(d."State Code")
WHERE EXISTS (SELECT port_code FROM public.staging_immigration i 
WHERE p.port_code = i.port_code)) t1
LEFT JOIN public.dim_demographics t2 ON t1.port_id=t2.port_id AND t1.race=t2.race
WHERE t2.port_id IS NULL
"""

insert_dim_time = """
INSERT INTO public.dim_time (sas_timestamp, year,month,day,quarter,week,day_of_week)
SELECT t1.ts, 
    date_part('year', t1.sas_date) as year,
    date_part('month', t1.sas_date) as month,
    date_part('day', t1.sas_date) as day, 
    date_part('quarter', t1.sas_date) as quarter,
    date_part('week', t1.sas_date) as week,
    date_part('dow', t1.sas_date) as day_of_week
FROM
    (SELECT DISTINCT arrdate as ts, TIMESTAMP '1960-01-01 00:00:00 +00:00' + (arrdate * INTERVAL '1 day') as sas_date
    FROM staging_immigration
    UNION
    SELECT DISTINCT depdate as ts, TIMESTAMP '1960-01-01 00:00:00 +00:00' + (depdate * INTERVAL '1 day') as sas_date
    FROM staging_immigration
    WHERE depdate IS NOT NULL
    ) t1
LEFT JOIN public.dim_time t2 ON t1.ts=t2.sas_timestamp
WHERE t2.sas_timestamp IS NULL
"""

insert_fact_immigration = """
INSERT INTO public.fact_immigration (country_id, port_id, age, travel_mode, visa_category, visa_type,
gender,birth_year,arrdate,depdate)
SELECT c.country_id, p.port_id, i.age, i.mode_of_travel, i.reason_for_travel, i.visatype, i.gender, i.birth_year,i.arrdate, i.depdate
FROM public.staging_immigration i
INNER JOIN public.dim_countries c ON i.country_code = c.country_code
INNER JOIN public.dim_ports p ON i.port_code = p.port_code
"""

#### Data Quality checks

# Make sure that relevant staging items make it to fact table
staging_to_fact_quality_check = """
SELECT s.stagingCount - f.factCount 
FROM
    (SELECT COUNT(i.*) as stagingCount
    FROM public.staging_immigration i
    INNER JOIN public.dim_countries c ON i.country_code = c.country_code
    INNER JOIN public.dim_ports p ON i.port_code = p.port_code) s
CROSS JOIN (SELECT COUNT(*) as factCount FROM fact_immigration i INNER JOIN dim_time t ON i.arrdate = t.sas_timestamp 
WHERE t.year={{ execution_date.year }} AND t.month={{execution_date.month}} and t.day={{ execution_date.day }}) f
"""

# Make sure items are in staging table
staging_count_quality_check = """
SELECT COUNT(*) FROM staging_immigration
"""


# staging table queries
drop_staging_table_queries = [drop_staging_immigration, drop_staging_countries_table, drop_staging_ports_table, drop_staging_airports_table, drop_staging_demographics_table, drop_staging_city_temps_table]

create_staging_table_queries = [create_staging_immigration_table, create_staging_countries_table, create_staging_ports_table, create_staging_airports_table, create_staging_demographics_table, create_staging_city_temps_table]

copy_table_queries = [staging_countries_copy, staging_ports_copy, staging_airports_copy, staging_demographics_copy, staging_immigration_copy, ]

# fact and dimension table queries
drop_fact_and_dim_table_queries = [drop_dim_countries_table, drop_dim_ports_table, drop_dim_airports_table, drop_dim_demographics_table, drop_dim_time_table, drop_fact_immigration_table]

create_fact_and_dim_table_queries = [create_dim_countries_table, create_dim_ports_table, create_dim_airports_table, create_dim_demographics_table, create_dim_time_table, create_fact_immigration_table]

insert_table_queries = [insert_dim_countries, insert_dim_ports, insert_dim_airports, insert_dim_demographics, insert_dim_time, insert_fact_immigration]

# combined create and drop table queries
create_table_queries = create_staging_table_queries + create_fact_and_dim_table_queries
drop_table_queries = drop_staging_table_queries + drop_fact_and_dim_table_queries

# data quality check queries
data_quality_queries = [staging_count_quality_check, staging_to_fact_quality_check]