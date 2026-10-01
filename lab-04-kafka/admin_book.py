from confluent_kafka.admin import AdminClient, NewTopic

config = {
    "bootstrap.servers": "localhost:9092"
}

admin_client = AdminClient(config)

topic = "book-lines"

# Create topic
futures = admin_client.create_topics([
    NewTopic(
        topic,
        num_partitions=1,
        replication_factor=1
    )
])

# Wait until Kafka finishes creating the topic
for topic_name, future in futures.items():
    try:
        future.result()
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"Topic '{topic_name}' could not be created: {e}")

# List existing topics
metadata = admin_client.list_topics(timeout=10)

print("\nAvailable topics:")

for topic_name in metadata.topics.keys():
    print("-", topic_name)