from accident.main import initInterface
from random import randint
from string import ascii_letters, digits
import json
from kafka.errors import KafkaError
from kafka import KafkaProducer
from decouple import config
import requests
from datetime import datetime

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
        self.now = self.__gps()
        self.username = "TS"
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

    
    def __gps(self)-> dict:
        ip_request = requests.get('https://get.geojs.io/v1/ip.json')
        my_ip = ip_request.json()['ip']
        geo_request = requests.get('https://get.geojs.io/v1/ip/geo/' +my_ip + '.json')
        geo_data = geo_request.json()
        return{
            'lat': float(geo_data['latitude'])+randint(1,99)/100,
            "lng": float(geo_data['longitude'])+randint(1,99)/100,
        }


    def send(self, topic, transaction):
        try:
            self.producer.send(topic, value=transaction)         
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


if __name__ == "__main__":
    transaction1 = Transaction()
    accident = initInterface(transaction1,"New accident")
    accident.run()