from kafka import KafkaConsumer
from decouple import config
import json

from threading import Thread
KAFKA_BROKER_URL = config('KAFKA_BROKER_IN_CAR_URL')
TRANSACTIONS_TOPIC_IN_CUSTOMER = config('TRANSACTIONS_TOPIC_IN_CUSTOMER')
USERNAME = config('USERNAME_IN_CAR')
PASSWORD = config('PASSWORD_IN_CAR')


class Consumer(Thread):
    def __init__(self, transactions):
        Thread.__init__(self)
        self.consumer = KafkaConsumer(
            TRANSACTIONS_TOPIC_IN_CUSTOMER,
            bootstrap_servers=KAFKA_BROKER_URL,
            value_deserializer=lambda value: json.loads(value),
            api_version=(0, 10, 1),
            # ver sasl plain
            # security_protocol='SASL_PLAINTEXT',
            # sasl_mechanism='PLAIN',
            # sasl_plain_username=USERNAME,           
            # sasl_plain_password=PASSWORD,
        )
        self.set_username = transactions.set_username
        self.send_dds = transactions.create_transaction_drowsiness
        self.response_time = transactions.response_time
        pass

    def receive_message(self):
        for message in self.consumer:
            transaction: dict= message.value
            if "condition" in transaction:
                if transaction["condition"] == 'set_account':
                    self.set_username(transaction["username"])
                    print("username changes")
                if transaction["condition"] == 'send_dds':
                    self.response_time = transaction["response_time"]
                    print("dds_send")
                    self.send_dds(transaction["response_time"])
                

    def run(self):
        print("consumer run")
        self.receive_message()
