from confluent_kafka import Consumer
import re


# Kafka configuration
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "book-cleaner-v1",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(conf)

topic = "book-lines"
consumer.subscribe([topic])

output_file = "alice_cleaned.txt"

MAX_EMPTY_POLLS = 10
empty_polls = 0

received_count = 0
written_count = 0


with open(output_file, "w", encoding="utf-8") as output:

    while True:
        msg = consumer.poll(1.0)

        # No new message
        if msg is None:
            empty_polls += 1

            if empty_polls >= MAX_EMPTY_POLLS:
                print("No more messages. Closing consumer.")
                break

            continue

        # Kafka error
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        empty_polls = 0
        received_count += 1

        # Decode Kafka message
        text = msg.value().decode("utf-8")

        # Basic text cleaning
        text = text.lower()

        # Remove punctuation and special characters
        text = re.sub(r"[^a-z0-9\s']", " ", text)

        # Replace multiple spaces by one space
        text = re.sub(r"\s+", " ", text).strip()

        # Ignore empty lines
        if text:
            output.write(text + "\n")
            written_count += 1

            # Display only the first 10 cleaned lines
            if written_count <= 10:
                print(f"Cleaned: {text}")


consumer.close()

print()
print(f"Messages received: {received_count}")
print(f"Cleaned lines written: {written_count}")
print(f"Output file: {output_file}")