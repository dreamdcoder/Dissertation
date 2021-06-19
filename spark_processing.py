# Import Libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
import json
import pickle
import pandas as pd
from pyspark.sql.types import StructType, IntegerType, DoubleType, StringType, TimestampType
import requests

# set Environment parameter (optional)'''
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1'
global path
path = os.getcwd() + '\model'
REST_API_URL = 'https://api.powerbi.com/beta/c8eca3ca-1276-46d5-9d9d-a0f2a028920f/datasets/b95e3666-7d39-4faf-b49a-aad8879f1021/rows?key=GMq1fua6vtBWckini28U0Lw06se3xhcvV4LgxZ1jJW%2F7jcki33AoqGsJXDZDXce%2BRbSacaSBi49acMNkDmNHNA%3D%3D'

# Build a spark session
spark = SparkSession \
    .builder \
    .appName("network") \
    .getOrCreate()

# Define Schema for incoming json data
user_schema = StructType() \
    .add("key", StringType()) \
    .add("time", TimestampType()) \
    .add("active-routes-count", DoubleType()) \
    .add("backup_routes_count", DoubleType()) \
    .add("deleted_routes_count", DoubleType()) \
    .add("paths_count", DoubleType()) \
    .add("performance_stat_global_config_items_processed", DoubleType()) \
    .add("performance_stat_vrf_inbound_update_messages", DoubleType()) \
    .add("protocol_route_memory", DoubleType()) \
    .add("total_neighbors_count", DoubleType()) \
    .add("vrf_path_count", DoubleType()) \
    .add("vrf_update_messages_received", DoubleType())

# read stream from kafka topic customer_location
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "data_plane") \
    .load()

# convert data to string format
raw_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# providing timestamp format for parsing
TimestampFormat = "yyyy-MM-dd HH:mm:ss.f"
jsonOptions = {"timestampFormat": TimestampFormat}

# explode single value column from stream to json schema columns
raw_df = raw_df.withColumn("value", from_json(col="value", schema=user_schema, options=jsonOptions)).select("value.*")


def process_row(row):
    print('------------------------------------------------------------')
    cols = ['key', 'time', 'active-routes-count', 'backup_routes_count', 'deleted_routes_count', 'paths_count',
            'performance_stat_global_config_items_processed',
            'performance_stat_vrf_inbound_update_messages',
            'protocol_route_memory', 'total_neighbors_count',
            'vrf_path_count', 'vrf_update_messages_received']
    df = pd.DataFrame(row).transpose()
    df.columns = cols
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df = df.astype({'active-routes-count': 'float64',
                    'backup_routes_count': 'float64', 'deleted_routes_count': 'float64',
                    'paths_count': 'float64',
                    'performance_stat_global_config_items_processed': 'float64',
                    'performance_stat_vrf_inbound_update_messages': 'float64',
                    'protocol_route_memory': 'float64', 'total_neighbors_count': 'float64',
                    'vrf_path_count': 'float64', 'vrf_update_messages_received': 'float64'})
    node_name =df["key"][0]
    # load scaler file
    print(type(node_name),node_name)
    scaler_file_name = node_name + "_scaler.pkl"
    spath = os.path.join(path, scaler_file_name)
    with open(spath, 'rb') as f:
        scaler = pickle.load(f)

    # load model file
    model_file_name = node_name + "_model.pkl"
    mpath = os.path.join(path, model_file_name)
    with open(mpath, 'rb') as f:
        model = pickle.load(f)

    # scaler = pickle.load(open('C:\\Users\\yogeshja\\Desktop\\Dissertation\\leaf1_scaler.pkl', 'rb'))

    df_new = df.drop(['key', 'time'], axis=1)
    df_new_scaled = scaler.transform(df_new)
    y_pred_outliers = model.predict(df_new_scaled)
    print(type(y_pred_outliers), y_pred_outliers)
    # df_dummy=
    # data_json = bytes(df.to_json(orient='records', date_format='iso', date_unit='ms'),encoding='utf-8')
    # req =requests.put(REST_API_URL,data_json)
    # print(req.text)
    # print(data_json)


query = raw_df \
    .writeStream \
    .trigger(processingTime='10 seconds') \
    .foreach(process_row) \
    .start()
query.awaitTermination()
