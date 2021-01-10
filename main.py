from accident.main import initInterface
from drowsiness.main import initInterfaceDDS
from consumer import Consumer
from random import randint
from string import ascii_letters, digits
import json
from kafka import KafkaProducer
from decouple import config
from datetime import datetime
from threading import Thread
import requests

KAFKA_BROKER_URL = config('KAFKA_BROKER_URL')
TRANSACTIONS_TOPIC = config('TRANSACTIONS_TOPIC')
TRANSACTIONS_PER_SECOND = 0.01
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND

class Transaction(Thread):
    def __init__(self):
        self.now = self.__gps()
        self.username = ""
        self.carID = config('CARID')
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            # Encode all values as JSON
            value_serializer=lambda value: json.dumps(value).encode(),
        )
        self.response_time = 0
        self.working_time = datetime.now()
    
    def __gps(self)-> dict:
        ip_request = requests.get('https://get.geojs.io/v1/ip.json')
        my_ip = ip_request.json()['ip']
        geo_request = requests.get('https://get.geojs.io/v1/ip/geo/' +my_ip + '.json')
        geo_data = geo_request.json()
        return{
            'lat': geo_data['latitude'],
            "lng": geo_data['longitude'],
        }

    def set_username(self,name):
        self.username = name
        self.working_time = datetime.now()


    def create_transaction_drowsiness(self,response_time):
        """Create a fake, randomised transaction."""
        self.now = self.__gps()
        transaction: dict = {
            'username': self.username,
            'carID': self.carID,
            'lat': self.now["lat"],
            "lng": self.now["lng"],
            'condition': 'DDS',
            'time': str(datetime.now()),
            'response_time': response_time,
            'working_time': str(datetime.now()- self.working_time),
        }
        self.producer.send(TRANSACTIONS_TOPIC, value=transaction) 
        print(transaction)

    
    def create_transaction_accident(self) :
        """Create a fake, randomised transaction."""
        self.now = self.__gps()
        transaction: dict = {
            'username': self.username,
            'carID': self.carID,
            'lat': self.now["lat"],
            "lng": self.now["lng"],
            'condition': 'ACS',
            'time': str(datetime.now()),
        }
        print(transaction)
        self.producer.send(TRANSACTIONS_TOPIC, value=transaction)

    def isSuspicious(transactions: dict) -> bool:
        return transactions['amount'] >= 900


if __name__ == "__main__":
    transaction1 = Transaction()
    accident = initInterface(transaction1)
    consumer = Consumer(transaction1)
    consumer.daemon = True
    consumer.start()
    accident.run()

        

    
    
