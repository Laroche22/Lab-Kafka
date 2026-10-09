from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

topic = "book-lines"

admin_client = AdminClient({
    "bootstrap.servers": "localhost:9092"
})

futures = admin_client.create_topics(
    [NewTopic(topic, num_partitions=1, replication_factor=1)],
    request_timeout=10
)

try:
    futures[topic].result(timeout=15)
    print(f"Topic '{topic}' created successfully.")
except KafkaException as error:
    if error.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
        print(f"Topic '{topic}' already exists.")
    else:
        raise