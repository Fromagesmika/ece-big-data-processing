from confluent_kafka import Producer
import socket

# Kafka configuration
conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": socket.gethostname()
}

producer = Producer(conf)

topic = "book-lines"
book_file = "alice.txt"

line_count = 0

# Read book line by line
with open(book_file, "r", encoding="utf-8") as file:
    for line in file:

        # Remove only the end-of-line character
        line = line.rstrip("\n")

        producer.produce(
            topic=topic,
            value=line
        )

        line_count += 1

        # Avoid displaying the whole book
        if line_count <= 10:
            print(f"Sent: {line}")

# Wait until all messages are sent
producer.flush()

print(f"\nFinished. {line_count} lines sent to topic '{topic}'.")