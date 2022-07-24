# Data Lake with Apache Spark
## Table of Contents
* [About the Project](#about-the-project)
* [Usage](#usage)
* [Project Structure](#project-structure)

<a name="about-the-project"/>

## About the Project
This project deals with data modeling for a music streaming app by 'Sparkify'. Sparkify has collected data of songs and user activity from their app in the form of JSON files and wants to analyze what songs users are listening to.


This project creates an ETL pipeline for data lake hosted on S3, loads data from S3, processes the data into analytics tables using Spark, and stores them back into S3. This Spark process will be deployed on EMR cluster using AWS.

We create fact and dimension tables; fact table having our quantitative business information (songs users are listening to) and dimension tables having descriptive attributes (song and user details etc.) related to fact table.

<a name="usage"/>

## Usage
`etl.py` is the main python file for this project which instantiates a spark process and creates an ETL pipeline using data lake on s3. We copy this `etl.py` to spark master ec2 instance and run.
```
/home/hadoop# /usr/bin/spark-submit --master yarn ./etl.py
```

<a name="project-structure"/>

## Project Structure
This repository includes a data folder which contains song_data and log_data. In addition to data folder project contains 5 files:

1. `etl_test.ipynb`: This notebook has been used in this project to test and run spark processes.
2. `etl.py`: This file creates the ETL pipeline, reads and processes data lake of song and log files, creates analytics data store of sparkify and stores back into s3 in parquet format.
3. `dl.cfg`: This file contains AWS KEY and SECRET which will be used in `etl.py`
