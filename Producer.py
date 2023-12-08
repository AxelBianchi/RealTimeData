import requests
from kafka import KafkaProducer
import json
import time


bootstrap_servers = 'localhost:9092'
topic = 'velib'

url = 'https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json'


producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda x: json.dumps(x).encode('utf-8'))

def collecte_et_envoi_donnees():
    while True:
        response = requests.get(url)
        data = response.json()
        #print(data)
        
        producer.send(topic, value=data)
        print("Données envoyées à Kafka \n",data)
        time.sleep(10)  
        

if __name__ == "__main__":
    collecte_et_envoi_donnees()