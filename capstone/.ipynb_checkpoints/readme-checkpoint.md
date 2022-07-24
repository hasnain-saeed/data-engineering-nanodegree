# Data Engineering Capstone
The objective of this capstone project is to develop a robust data warehouse having a single source of data coming from different data sources, clean the data and process it through ETL pipeline to generate a usable dataset for analytics purposes. This project is part of Udacity's Nano Degree program in which the data warehousing techniques learned during the degree are utilized.

### Scope of the Project
In this project:

- Data is extracted from the immigration SAS data, partitioned by year, month and day and stored as parquet files.
- These partitioned parquet files are then loaded into redshift staging tables
- The staging data combined with other staged data sources produces the final star schema model in redshift data warehourse which will be used for analytics purposes.

I worked on udacity provided dataset of immigration, demographics, airports and temperature and will be looking into the relationship between the:

- Countries of immigrants and a specific port city.
- Demographics of port city and country of origin.
- Average temperature of country of origin and port city of entry.
- Inflow of immigrations in a given time of year or month and certain areas.

### Datasets
- **I94 Immigration Data**: This data comes from the US National Tourism and Trade Office. 
- **World Temperature Data**: This dataset came from Kaggle.
- **U.S. City Demographic Data** : This data comes from OpenSoft.
- **Airport Code Table**: This is a simple table of airport codes and corresponding cities. 

### Data Modeling and Data Dictionary

This warehouse consists of 6 staging tables, 5 dimension tables and a fact table. Schemas are given below:

#### Data Model Diagram

![Data Model Diagram](./images/dbdiagram.png)

#### Data Dictionary

##### dim_countries

|Field|Type|Description|
|----|-----|-----------|
|country_id|BIGINT|Primary key|
|country_code|VARCHAR(3)|3 character code for country|
|country| VARCHAR(256) |Country name|
|average_temperature|NUMERIC(16,3)|Average temperature of country|



##### dim_ports

|Field|Type|Description|
|----|-----|-----------|
|port_id|BIGINT|Primary key|
|port_code|VARCHAR(3)|3 character code for port|
|port_city|VARCHAR(256)|City of port|
|port_state|VARCHAR(50)|State of port|
|average_temperature|NUMERIC(16,3)|Average temperature of port city|



##### dim_airports

|Field|Type|Description|
|----|-----|-----------|
|airport_id|BIGINT|Primary key|
|port_id|BIGINT|Foreign key to dim_port|
|airport_type|VARCHAR(256)|Short description of airport type|
|airport_name|VARCHAR(256)|Airport name|
|elevation_ft|INTEGER|Airport elevation|
|municipality|VARCHAR(256)|Airport municipality|
|gps_code|VARCHAR(256)|Airport GPS code|
|iata_code|VARCHAR(256)|Airport International Air Transport Association code|
|local_code|VARCHAR(256)|Airport local code|
|coordinates|VARCHAR(256)|Airport coordinates|

##### dim_demographics

|Field|Type|Description|
|----|-----|-----------|
|demographics_id|BIGINT|Primary key|
|port_id|BIGINT|Foreign key to dim_port|
|median_age|NUMERIC(18,2)|Median age for demographic record|
|male_population|INTEGER|Number of male population in city|
|female_population|INTEGER|Number of female population in city|
|total_population|BIGINT|Total number of people in city|
|number_of_veterans|INTEGER|Total number of veterans in city|
|foreign_born|INTEGER|Total number of foreign born in city|
|avg_household_size|NUMERIC(18,2)|Average household size in city|
|race|VARCHAR(100)|Race for this demographic|
|demo_count|INTEGER|Demographic count|

##### dim_time

|Field|Type|Description|
|----|-----|-----------|
|sas_timestamp|INTEGER|Primary key & timestamp (days since 1/1/1960)|
|year|INTEGER|4 digit year|
|month|INTEGER|Month between 1-12|
|day|INTEGER|Day between 1-31|
|week|INTEGER|Week of year between 1-52|
|day_of_week|INTEGER|Day of week between 1-7|
|quarter|INTEGER|Quarter of year between 1-4|

##### fact_immigrations

|Field|Type|Description|
|----|-----|-----------|
|immigration_id|BIGINT|Primary key|
|country_id|BIGINT|Foreign key to dim_countries|
|port_id|BIGINT|Foreign key to dim_port|
|age|INTEGER|Age of immigrant|
|travel_mode|VARCHAR(100)|Mode of travel of immigrant (Air, Sea, Land)|
|visa_category|VARCHAR(100)|Category of immigrant visa|
|visa_type|VARCHAR(100)|VISA type of immigrant|
|gender|VARCHAR(10)|Immigrant gender|
|birth_year|INTEGER|Immigrant birth year|
|arrdate|INTEGER|Arrival date & Foreign key to dim_time|
|depdate|INTEGER|Departure date & Foreign key to dim_time|

### How to run the project?

The project includes 3 python executable files:
- process_immigration_data.py: It is run by spark instance to extract transform and load the data to s3 data lake. It uses immigration dataset and sas mapping to generate partitioned immigration and city temperature dataset. It also uses sas mapping to generate ports and countries csv datasets.
- create_tables.py: This file drop staging and fact and dimension tables if they exist and create new staging and fact and dimension tables on redshift using sql queries in sql_queries.py.
- etl.py: This file loads the data into redshift staging tables from s3 data lakes and inserts the data into fact and dimension tables from staging tables. It finally runs data quality checks on the staging and fact table.

This project includes sql queries in sql_queries.py

### Data Quality Checks

There are two data quality checks in sql_queries.py used in etl.py.

* staging_to_fact_quality_check - This data quality check ensures that the right amount of data was added to our immigration fact table from the staging table. It compares the count of records for a particular day in the fact table and the count of records in the staging table. 

* staging_count_quality_check - This data quality check ensures that after staging the immigration data for a particular data from Amazon S3 to Amazon Redshift that we have records in the staging table.

Having these checks in airflow DAG will ensure that the data pipeline is working as expected.

In addition to these two checks, foreign key constraints have been added to ensure data integrity between the fact and dimension tables.


### Tools and Technologies
  
In this project, I used [Amazon S3](https://aws.amazon.com/s3/) to store CSV and partitioned dataset coming from [Apache Spark](https://aws.amazon.com/emr/) and [Amazon Redshift](https://aws.amazon.com/redshift/) to build our primary data warehouse.

#### Why using these technologies?

- Amazon S3 - There are plenty of options we can use to store our dataset but I used [Amazon S3](https://aws.amazon.com/s3/) because of its scalability, data availability, security, and performance all in one place. Also it is very convenient to use S3 and redshift together.

- Apache Spark - I used Spark primarily to extract, clean, and partition the immigration data. The immigrations files are so large that I decided to preprocess the data through Spark.

- Amazon Redshift - Building a data warehouse can be overwhelming while choosing the right technologies. We have multiple options available from different big tech companies like Google BigQuery, Redshift and Snowflake etc. I used Redshift because of the its performance and compatibility with S3.



### Scenarios

#### If the data was increased by 100x.
If the data were increased 100x, then we should be able to easily handle it as we are using Spark and Redshift and scaling the clusters as needed will do the job. AWS provides multiple options for example running self managed Spark instance on EC2 or fully managed EMR cluster without worrying about the overhead. Also EMR storage layer includes the different file systems like HDFS, EMRFS etc. S3 can also be used as staging area.

#### If the pipelines were run on a daily basis by 7am.
Apache airflow was not used in this project but it is a perfect candidate for managing and running the pipelines in regular intervals. This can easily be achieved by setting a daily interval to 7 AM. We can also provide a crontab syntax of `0 7 * * *` in the `schedule_interval` argument of the `DAG`

#### If the database needed to be accessed by 100+ people
As we are making use of redshift in our project, with the concurrency scaling feature, it can support virtually unlimited concurrent users and concurrent queries, with consistently fast query performance.

 
### Example Data Usage

```sql
-- Top 10 countries with most number of immigrants arriving by country in years between 2014-2016
SELECT c.country, COUNT(*) FROM fact_immigration i AS number_of_immigrants
INNER JOIN dim_countries c ON i.country_id = c.country_id
INNER JOIN dim_time t ON i.arrdate=t.sas_timestamp
WHERE t.year>=2014 AND t.year<=2016
GROUP BY c.country
ORDER BY number_of_immigrants DESC
LIMIT 10
```
