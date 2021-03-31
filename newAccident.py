from accident.main import initInterface
from consumer import Consumer
from random import randint
from string import ascii_letters, digits
import json
from kafka.errors import KafkaError
from kafka import KafkaProducer
from decouple import config
import time
from threading import Thread

KAFKA_BROKER_URL = config('KAFKA_HOST_CAR')
TRANSACTIONS_TOPIC1= config('KAFKA_AIC_TOPIC')
DDS_TRANSACTIONS_TOPIC = config('KAFKA_DIC_TOPIC')
USERNAME_TRANSACTIONS_TOPIC = config('KAFKA_USERNAME_TOPIC')
TRANSACTIONS_PER_SECOND = 0.01
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND
USERNAME = config('USERNAME_IN_CAR')
PASSWORD = config('PASSWORD_IN_CAR')
carID = config('CAR_ID')
class Transaction():
    def __init__(self):
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
        self.countDown = time.time()
        self.isStart = 1

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
        transaction: dict = {
            'condition': 'AIC',
            'carID': carID,
        }
        print(transaction)
        t = time.time()
        print(t-self.countDown)
        if(t-self.countDown > 3600 or self.isStart):
            print("----send-----")
            self.countDown = t
            self.isStart = 0
            self.send(TRANSACTIONS_TOPIC1, transaction)


if __name__ == "__main__":
    transaction1 = Transaction()
    transaction1.send(TRANSACTIONS_TOPIC1,{ 'condition': 'hs'})
    transaction1.send(DDS_TRANSACTIONS_TOPIC,{ 'condition': 'hs'})
    transaction1.send(USERNAME_TRANSACTIONS_TOPIC,{ 'condition': 'hs'})
    accident = initInterface(transaction1,"Detect accident")
    accident.run()