from datetime import datetime
from random import randrange
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time
from data_gen import DataGen
from kafka.errors import KafkaError


def create_topic(topic_name):
    # create a consumer object
    consumer = KafkaConsumer(bootstrap_servers="localhost:9092")

    # create topic if not present
    if 'name' in consumer.topics():
        print("Topic {} already present".format(topic_name))

    else:
        admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092",
            client_id='test'
        )
    try:
        topic_list = []
        topic_list.append(NewTopic(name=topic_name, num_partitions=8, replication_factor=1))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print("Topic {} created".format(topic_name))
    except KafkaError:
        print("Topic {} already present".format(topic_name))
    # create a producer object
    consumer.close()


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json_serializer)

if __name__ == "__main__":
    create_topic('data_plane')
    leaf1 = DataGen('leaf1')

    while 1 == 1:
        record = leaf1.prepare_record()
        print(record)
        producer.send("data_plane", key=bytes(record["key"], 'utf-8'), value=record)
        time.sleep(100)
