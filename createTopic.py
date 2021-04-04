from kafka.admin import KafkaAdminClient, NewTopic
from decouple import config

KAFKA_BROKER_URL = config('KAFKA_HOST')
TRANSACTIONS_TOPIC = config('KAFKA_ACD_TOPIC')
DDS_TOPIC = config('KAFKA_DDS_TOPIC')
USERNAME = config('USERNAME_KAFKA_ON_CLN')
PASSWORD = config('PASSWORD_KAFKA_ON_CLN')
carID = config('CAR_ID')

admin_client = KafkaAdminClient(
    bootstrap_servers=KAFKA_BROKER_URL,
    api_version=(0, 10, 1),
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username=USERNAME,           
    sasl_plain_password=PASSWORD,
)

topic_list = []
topic_list.append(NewTopic(name=TRANSACTIONS_TOPIC, num_partitions=1, replication_factor=1))
topic_list.append(NewTopic(name=DDS_TOPIC, num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)