import socket
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}

producer = Producer(conf)
topic = 'book-lines'
book_path = 'book.txt'

def delivery_report(err, msg):
    if err is not None:
        print(f"Échec de l'envoi : {err}")

with open(book_path, 'r', encoding='utf-8') as f:
    for line in f:
        line_clean = line.strip()
        if not line_clean:
            continue
        producer.poll(0)
        producer.produce(
            topic=topic,
            value=line_clean.encode('utf-8'),
            callback=delivery_report
        )

producer.flush()
print("Livre envoyé avec succès dans le topic !")