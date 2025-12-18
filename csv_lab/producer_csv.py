"""
CSV Producer for Kafka Streaming Lab
This producer reads CSV file line by line and streams each row as a Kafka message.
"""

from kafka import KafkaProducer
import time
import os

def create_producer():
    """Create and configure Kafka producer"""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: v.encode('utf-8')
    )
    return producer

def stream_csv_to_kafka(csv_file_path, topic_name, delay=0.1):
    """
    Stream CSV file to Kafka topic.

    Args:
        csv_file_path: Path to CSV file
        topic_name: Kafka topic name
        delay: Delay between messages (seconds) to simulate streaming
    """
    producer = create_producer()

    try:
        with open(csv_file_path, 'r') as file:
            # Read header
            header = file.readline().strip()
            print(f"📊 Header: {header}")

            # Send header as first message
            producer.send(topic_name, value=header)
            print(f"✓ Sent header to topic '{topic_name}'")

            # Stream data rows
            row_count = 0
            for line in file:
                line = line.strip()
                if line:  # Skip empty lines
                    producer.send(topic_name, value=line)
                    row_count += 1
                    print(f"✓ Sent row {row_count}: {line}")
                    time.sleep(delay)  # Simulate streaming with delay

            # Ensure all messages are sent
            producer.flush()
            print(f"\n🎉 Successfully streamed {row_count} rows to topic '{topic_name}'")

    except FileNotFoundError:
        print(f"❌ Error: File '{csv_file_path}' not found")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    # Configuration
    TOPIC_NAME = "transactions-csv"
    CSV_FILE = os.path.join(os.path.dirname(__file__), "data", "transactions.csv")

    print("=" * 60)
    print("🚀 Starting CSV Producer")
    print("=" * 60)
    print(f"📁 CSV File: {CSV_FILE}")
    print(f"📮 Topic: {TOPIC_NAME}")
    print("=" * 60)

    # Stream CSV to Kafka
    stream_csv_to_kafka(CSV_FILE, TOPIC_NAME, delay=0.1)
