from accident.main import initInterface
from consumerFinal import Consumer
from random import randint
from string import ascii_letters, digits
import json
from kafka.errors import KafkaError
from kafka import KafkaProducer
from decouple import config
from datetime import datetime
from threading import Thread
import requests

KAFKA_BROKER_URL = config('KAFKA_HOST')
TRANSACTIONS_TOPIC = config('KAFKA_ACD_TOPIC')
DDS_TOPIC = config('KAFKA_DDS_TOPIC')
TRANSACTIONS_PER_SECOND = 0.01
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND
USERNAME = config('USERNAME_KAFKA_ON_CLN')
PASSWORD = config('PASSWORD_KAFKA_ON_CLN')
carID = config('CAR_ID')
class Transaction():
    def __init__(self):
        # Thread.__init__(self)
        self.now = self.__gps()
        self.username = ""
        self.carID = carID
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            api_version=(0, 10, 1),
            # ver sasl plain
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username=USERNAME,           
            sasl_plain_password=PASSWORD,
            value_serializer=lambda value: json.dumps(value).encode(),
        )
        self.response_time = 0
        self.working_time = datetime.utcnow()
    
    def __gps(self)-> dict:
        pos = requests.get('http://localhost:4000/position')
        p = pos.json()
        if(p["successful"]):
            return p["data"]
        else:
            self.__gps()

    def set_username(self,name):
        self.username = name
        self.working_time = datetime.utcnow()


    def create_transaction_drowsiness(self,response_time):
        """Create a fake, randomised transaction."""
        self.now = self.__gps()
        transaction: dict = {
            'username': self.username,
            'carID': self.carID,
            'lat': self.now["lat"],
            "lng": self.now["lng"],
            'condition': 'DDS',
            'time':  str(datetime.utcnow().isoformat())+"Z",
            'response_time': response_time,
            'working_time': (datetime.utcnow()- self.working_time).total_seconds()/3600,
        }
        print(transaction)
        self.send(DDS_TOPIC,transaction)

    def send(self, topic, transaction):
        try:
            self.producer.send(topic, value=transaction)         
        except KafkaTimeoutError as kte:
            print(kte)
            self.send(topic,transaction)
        except KafkaError as ke:
            print(ke)
            self.send(topic,transaction)
        except Exception as e:
            print(e)
            self.send(topic,transaction)

    
    def create_transaction_accident(self) :
        """Create a fake, randomised transaction."""
        self.now = self.__gps()
        transaction: dict = {
            'username': self.username,
            'carID': self.carID,
            'lat': self.now["lat"],
            "lng": self.now["lng"],
            'condition': 'ACS',
            'time':  str(datetime.utcnow().isoformat())+"Z",
        }
        print(transaction)
        self.send(TRANSACTIONS_TOPIC, transaction)

    def isSuspicious(transactions: dict) -> bool:
        return transactions['amount'] >= 900


if __name__ == "__main__":
    transaction1 = Transaction()
    # accident = initInterface(transaction1,"Accident from this car")
    consumer = Consumer(transaction1)
    # consumer.daemon = True
    consumer.start()
    # transaction1.run()
