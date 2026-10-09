import re
from confluent_kafka import Consumer

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'book-consumer-group',
    'auto.offset.reset': 'earliest'
}

TOPIC = 'book-lines'
OUTPUT_FILE = 'cleaned_book.txt'

MAX_EMPTY_POLLS = 10
MAX_ERRORS = 5


def clean_text(text):
    """Clean and normalize each received line."""
    text = text.strip()
    text = re.sub(r'[\x00-\x08\x0b-\x1f\x7f]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text


consumer = Consumer(conf)
consumer.subscribe([TOPIC])

message_count = 0
empty_polls = 0
error_count = 0

print(f"Subscribed to topic: {TOPIC}")
print("Waiting for messages...")

try:
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as output:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                empty_polls += 1

                if empty_polls >= MAX_EMPTY_POLLS:
                    print("No new messages. Stopping consumer.")
                    break

                continue

            if msg.error():
                error_count += 1
                print(f"Consumer error: {msg.error()}")

                if error_count >= MAX_ERRORS:
                    print("Too many errors. Stopping consumer.")
                    break

                continue

            empty_polls = 0
            error_count = 0

            text = msg.value().decode('utf-8', errors='replace')
            cleaned_text = clean_text(text)

            if not cleaned_text:
                continue

            output.write(cleaned_text + '\n')
            output.flush()

            message_count += 1

    print(f"Messages processed: {message_count}")
    print(f"Output saved to: {OUTPUT_FILE}")

finally:
    consumer.close()