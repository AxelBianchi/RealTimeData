from kafka import KafkaConsumer
import json

bootstrap_servers = 'localhost:9092'
topic = 'velib'

consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

for message in consumer:
    data = message.value
    print(data)  