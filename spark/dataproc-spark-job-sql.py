#!/usr/bin/env python
# coding: utf-8


"""
Command-line for submiting job:

gcloud dataproc jobs submit pyspark \
  --cluster=<culster-name> \
  --region=<region> \
  gs://<path-to-python-file> \
  -- \
  --input=gs://<path-to-file> \
  --output=gs://<path-to-dir>
"""

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument('--input', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input = args.input
output = args.output


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

#gs://spark-2025-pq
df_trips= spark.read.parquet(input)

df_trips.registerTempTable('trips_data')


df_result = spark.sql("""
    SELECT MAX(
        (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600   
    )
    FROM trips_data  
""")

df_result.coalesce(1) \
    .write.parquet(output, mode='overwrite')



