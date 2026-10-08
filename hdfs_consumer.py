from kafka import KafkaConsumer
from json import loads
from rich import print
import pydoop.hdfs as hdfs
import datetime

# Kafka consumer
consumer = KafkaConsumer(
    'my-topic-test',              # Your topic
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],  # Your Kafka broker(s)
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=None,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

print("[INFO] Kafka consumer started...")
hdfs_path = '/data/twitter/raw/all_tweets.json'

# Process incoming messages
for message in consumer:
    tweet = message.value
    print(tweet)


    try:
        with hdfs.open(hdfs_path, 'at') as file:
            file.write(f"{tweet}\n")
            print("[INFO] Stored in HDFS!")
    except Exception as e:
        print(f"[ERROR] Failed to write to HDFS: {e}")