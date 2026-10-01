\# Kafka Streaming Lab



\## Objective



This lab demonstrates a Kafka producer/topic/consumer pipeline using

the `confluent-kafka` Python library.



\## Demo



The original demo uses the `timer` topic:



\- `admin.py` creates the topic

\- `producer.py` sends the current time

\- `consumer.py` consumes the messages



\## Book streaming pipeline



For the lab, \*\*Alice's Adventures in Wonderland\*\* by Lewis Carroll

from Project Gutenberg was used.



A new Kafka topic named `book-lines` was created.



\### Producer



`producer\_book.py` reads `alice.txt` line by line and sends each line

as an individual Kafka message.



\- Messages sent: 3762



\### Consumer



`consumer\_book.py` consumes the messages from the `book-lines` topic

and performs basic text cleaning:



\- convert text to lowercase

\- remove punctuation and special characters

\- remove extra spaces

\- ignore empty lines



The cleaned text is written to `alice\_cleaned.txt`.



\- Messages received: 3762

\- Cleaned lines written: 2795



\## Pipeline



`alice.txt → Producer → Kafka topic → Consumer → alice\_cleaned.txt`



\## Technologies



\- Apache Kafka 4.1.1

\- Docker

\- Python 3.11

\- confluent-kafka

