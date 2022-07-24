import pandas as pd
import configparser
import re
import os
import datetime
import calendar
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, DateType
from pyspark.sql.window import Window

config = configparser.ConfigParser()
config.read_file(open('capstone.cfg'))

os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']

I94_INPUT_PATH = config['WORKSPACE']['I94_INPUT_PATH']
TEMPERATURE_INPUT_PATH = config['WORKSPACE']['TEMPERATURE_INPUT_PATH']
OUTPUT_PATH = config['S3']['DATA_LAKE_PATH']
SAS_MAPPING_PATH = config['WORKSPACE']['SAS_MAPPING_PATH']

def create_spark_session():
    spark = SparkSession\
            .builder\
            .appName("i94 Data Extraction")\
            .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
            .enableHiveSupport().getOrCreate()
    return spark


def process_sas_mapping(mapping_path):
    """
    Process the mapping file to extract the relevant information
    :mapping_path: Input path to SAS data mapping
    """
    with open(mapping_path, "r") as f:
        file = f.read()
        sas_data = {}
        temp_data = []
        for line in file.split("\n"):
            line = re.sub(r"\s+", " ", line)
            if "/*" in line and "-" in line:
                label, description = [i.strip(" ") for i in line.split("*")[1].split("-", 1)]
                label = label.replace(' & ', '_').lower()
                sas_data[label] = {'description': description}
            elif '=' in line and ';' not in line:
                temp_data.append([i.strip(' ').strip("'").title() for i in line.split('=')])
            elif len(temp_data) > 0:
                sas_data[label]['data'] = temp_data
                temp_data = []
        return sas_data


def process_sas_date(days):
    """
    Converts SAS date stored as days since 1/1/1960 to datetime
    :days: Days since 1/1/1960
    :return: datetime
    """
    if days is None:
        return None
    return datetime.date(1960, 1, 1) + datetime.timedelta(days=days)


def process_sas_day(days):
    """
    Converts SAS date stored as days since 1/1/1960 to day of month
    :days: Days since 1/1/1960
    :return: Day of month value as integer
    """
    if days is None:
        return None
    return (datetime.date(1960, 1, 1) + datetime.timedelta(days=days)).day


def process_immigration_data(spark, sas_mapping, input_path, output_path):
    """
    Loads SAS i94 immigration data into data frame.
    Data is cleaned and projected, and then written to Parquet.
    Partitioned by year, month, and day.
    :spark: Spark session
    :input_path: Input path to SAS data
    :output_path: Output path for Parquet files
    :return: None
    """
    visas_df = spark.createDataFrame(pd.DataFrame(sas_mapping['i94visa']['data'], columns=['code', 'reason_for_travel']))
    visas_df = visas_df.withColumn('code', visas_df['code'].cast(IntegerType()))
    modes_df = spark.createDataFrame(pd.DataFrame(sas_mapping['i94mode']['data'], columns=['code', 'mode_of_travel']))
    modes_df = modes_df.withColumn('code', modes_df['code'].cast(IntegerType()))
    
    process_sas_date_udf = F.udf(process_sas_date, DateType())
    process_sas_day_udf = F.udf(process_sas_day, IntegerType())

    input_path = f"{input_path}/i94_jan16_sub.sas7bdat"

    df = spark.read.format("com.github.saurfang.sas.spark").load(input_path)

    # Set appropriate names and data types for columns
    df = df.withColumn('arrival_date', process_sas_date_udf(df['arrdate'])) \
        .withColumn('departure_date', process_sas_date_udf(df['depdate'])) \
        .withColumn('year', df['i94yr'].cast(IntegerType())) \
        .withColumn('month', df['i94mon'].cast(IntegerType())) \
        .withColumn('arrival_day', process_sas_day_udf(df['arrdate'])) \
        .withColumn('age', df['i94bir'].cast(IntegerType())) \
        .withColumn('country_code', df['i94cit'].cast(IntegerType()).cast(StringType())) \
        .withColumn('port_code', df['i94port'].cast(StringType())) \
        .withColumn('birth_year', df['biryear'].cast(IntegerType())) \
        .withColumn('mode', df['i94mode'].cast(IntegerType())) \
        .withColumn('visa', df['i94visa'].cast(IntegerType()))

    # Project final data set
    immigration_df = df.join(visas_df, df.visa ==  visas_df.code, "inner")\
                        .join(modes_df, df.mode == modes_df.code, "inner")\
                        .select(['year', 'month', 'arrival_day', 'age', 'country_code', 'port_code', 'mode_of_travel', 'reason_for_travel', 'visatype', 'gender', 'birth_year', 'arrdate', 'arrival_date', 'depdate', 'departure_date'])
    
    # Write data in Apache Parquet format partitioned by year, month, and day
    immigration_df.write.mode("append").partitionBy("year", "month", "arrival_day").parquet(f"{output_path}/immigration_data")


def process_city_temperature_data(spark, input_path, output_path):
    """
    Load GlobalLandTemperaturesByCity
    Extract latest temperature for U.S. cities
    Write to Parquet format
    :spark: Spark Session
    :input_path: Input path for temperature data
    :output_path: Output path for parquet files
    :return:
    """
    temperature_df = spark.read.option("header", True).option("inferSchema",True).csv(f"{input_path}/GlobalLandTemperaturesByCity.csv")
    temperature_df = temperature_df.filter(temperature_df.AverageTemperature.isNotNull())
    temperature_df = temperature_df.filter(temperature_df.Country == "United States") \
        .withColumn("rank", F.dense_rank().over(Window.partitionBy("City").orderBy(F.desc("dt"))))
    temperature_df = temperature_df.filter(temperature_df["rank"] == 1).orderBy("City")
    temperature_df.write.mode("overwrite").parquet(f"{output_path}/us_city_temperature_data")


def process_ports(sas_mapping):
    ports_df = pd.DataFrame(sas_mapping['i94port']['data'], columns=['port_code', 'port_of_entry'])
    ports_df['port_code'] = ports_df['port_code'].str.upper()
    ports_df['port_of_entry'] = ports_df["port_of_entry"].replace(to_replace=["No Port Code.*", "Collapsed.*"], value="Other,Other", regex=True)
    ports_df[['city', 'state']] = ports_df['port_of_entry'].str.rsplit(',', 1, expand=True)
    ports_df['city'] = ports_df['city'].str.upper()
    ports_df['state'] = ports_df['state'].str.strip()
    ports_df['state'] = ports_df['state'].str.upper()
    ports_df.drop(['port_of_entry'], axis=1, inplace=True)
    ports_df.to_csv(f'{output_path}/ports.csv', sep=',', index=False, storage_options={
        'key': config['AWS']['AWS_ACCESS_KEY_ID'], 
        'secret': config['AWS']['AWS_SECRET_ACCESS_KEY']
    })

    
def process_countries(sas_mapping):
    countries_df = pd.DataFrame(sas_data['i94cit_i94res']['data'], columns=['country_code', 'country'])
    countries_df['country'] = countries_df["country"].replace(to_replace=["No Country.*", "INVALID.*", "Collapsed.*", "Invalid.*"], value="Other", regex=True)
    countries_df['country'] = countries_df['country'].str.upper()
    countries_df.to_csv(f'{output_path}/countries.csv', sep=',', index=False, storage_options={
        'key': config['AWS']['AWS_ACCESS_KEY_ID'], 
        'secret': config['AWS']['AWS_SECRET_ACCESS_KEY']
    })
    
    
def main():
    spark = create_spark_session()
    sas_mapping = process_sas_mapping(SAS_MAPPING_PATH)
    process_immigration_data(spark, sas_mapping, I94_INPUT_PATH, OUTPUT_PATH)
    process_city_temperature_data(spark, TEMPERATURE_INPUT_PATH, OUTPUT_PATH)
    process_ports(sas_mapping)
    process_countries(sas_mapping)
    
if __name__ == "__main__":
    main()