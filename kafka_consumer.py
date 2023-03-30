import pandas as pd
from kafka import KafkaConsumer
from json import dumps,loads
import json
from time import sleep
from s3fs import S3FileSystem

consumer = KafkaConsumer('demo_testing2',bootstrap_servers=['99.80.23.223:9092'],value_deserializer = lambda x: loads(x.decode('utf-8')))

s3 = S3FileSystem()

for c,i in enumerate(consumer):

    with (s3.open(f"s3://kafka-stock-analysis-bucket/stock_json_file{c}","w")) as file:
        json.dump(i.value,file)
   
