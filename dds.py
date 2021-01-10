from drowsiness.main import initInterfaceDDS
from string import ascii_letters, digits
import json
from kafka import KafkaProducer
from decouple import config
from threading import Thread

KAFKA_BROKER_URL = config('KAFKA_BROKER_IN_CAR_URL')
TRANSACTIONS_TOPIC_IN_CUSTOMER = config('TRANSACTIONS_TOPIC_IN_CUSTOMER')
TRANSACTIONS_PER_SECOND = 0.01
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND

class Transaction(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            # Encode all values as JSON
            value_serializer=lambda value: json.dumps(value).encode(),
        )
        self.response_time = 0

    def create_transaction_drowsiness(self,t):
        """Create a fake, randomised transaction."""
        transaction: dict = {
            'condition': 'send_dds',
            'response_time': t,
        }
        self.producer.send(TRANSACTIONS_TOPIC_IN_CUSTOMER, value=transaction) 
        print(transaction)


if __name__ == "__main__":
    transaction1 = Transaction()
    drowsiness = initInterfaceDDS(transaction1)
    drowsiness.run()
    
