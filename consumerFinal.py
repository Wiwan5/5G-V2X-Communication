from kafka import KafkaConsumer
from decouple import config
import json
import time
from threading import Thread
KAFKA_BROKER_URL = config('KAFKA_HOST_CAR')
TRANSACTIONS_TOPIC = config('KAFKA_AIC_TOPIC')
DDS_TRANSACTIONS_TOPIC = config('KAFKA_DIC_TOPIC')
USERNAME_TRANSACTIONS_TOPIC = config('KAFKA_USERNAME_TOPIC')
USERNAME = config('USERNAME_KAFKA_IN_CAR')
PASSWORD = config('PASSWORD_KAFKA_IN_CAR')
Set_up_time_act = 900
carID = config('CAR_ID')
class Consumer(Thread):
    def __init__(self, transactions):
        Thread.__init__(self)
        self.consumer = KafkaConsumer(
            TRANSACTIONS_TOPIC,
            DDS_TRANSACTIONS_TOPIC,
            USERNAME_TRANSACTIONS_TOPIC,
            bootstrap_servers=KAFKA_BROKER_URL,
            value_deserializer=lambda value: json.loads(value),
            api_version=(0, 10, 1),
            # ver sasl plain
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='PLAIN',
            sasl_plain_username=USERNAME,           
            sasl_plain_password=PASSWORD,      
        )
        self.set_username = transactions.set_username
        self.send_dds = transactions.create_transaction_drowsiness
        self.response_time = transactions.response_time
        self.send_act = transactions.create_transaction_accident
        self.time = time.time()
        self.first_time_act = True
        pass

    def receive_message(self):
        for message in self.consumer:
            transaction: dict= message.value
            print(transaction)
            if "condition" in transaction:
                if "carID" in transaction and carID == transaction["carID"]:
                    if transaction["condition"] == 'set_account':
                        self.set_username(transaction["username"])
                        print("username changes")
                    if transaction["condition"] == 'DIC':
                        self.response_time = transaction["response_time"]
                        print("dds_send")
                        self.send_dds(transaction["response_time"])
                    if transaction["condition"] == 'AIC':
                        print("act_send")
                        if(self.first_time_act or time.time()-self.time>Set_up_time_act):
                            self.time = time.time()
                            self.first_time_act = False
                            self.send_act()

    def run(self):
        self.receive_message()
