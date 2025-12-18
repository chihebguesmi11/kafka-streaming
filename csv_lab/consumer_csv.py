"""
CSV Consumer for Kafka Streaming Lab
This consumer reads CSV messages from Kafka and displays them with offset tracking.
"""

from kafka import KafkaConsumer
import sys

def create_consumer(topic_name, group_id="csv-consumer-group", auto_offset_reset='earliest'):
    """
    Create and configure Kafka consumer.

    Args:
        topic_name: Kafka topic to consume from
        group_id: Consumer group ID
        auto_offset_reset: Where to start reading ('earliest' or 'latest')
    """
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode('utf-8')
    )
    return consumer

def consume_csv_from_kafka(topic_name, group_id="csv-consumer-group"):
    """
    Consume CSV messages from Kafka topic.

    Args:
        topic_name: Kafka topic name
        group_id: Consumer group ID
    """
    consumer = create_consumer(topic_name, group_id)

    print("=" * 70)
    print(f"👂 Consumer started - Listening to topic '{topic_name}'")
    print(f"👥 Consumer Group: {group_id}")
    print("=" * 70)
    print("Press Ctrl+C to stop\n")

    try:
        message_count = 0
        for message in consumer:
            message_count += 1

            # Display message details
            print(f"{'='*70}")
            print(f"📩 Message #{message_count}")
            print(f"├─ Partition: {message.partition}")
            print(f"├─ Offset: {message.offset}")
            print(f"├─ Timestamp: {message.timestamp}")
            print(f"└─ Value: {message.value}")
            print()

    except KeyboardInterrupt:
        print("\n" + "="*70)
        print(f"⏹️  Consumer stopped. Total messages consumed: {message_count}")
        print("="*70)
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        consumer.close()
        print("✓ Consumer closed gracefully")

if __name__ == "__main__":
    # Configuration
    TOPIC_NAME = "transactions-csv"
    GROUP_ID = "csv-consumer-group"

    # Allow custom group ID via command line
    if len(sys.argv) > 1:
        GROUP_ID = sys.argv[1]
        print(f"Using custom consumer group: {GROUP_ID}\n")

    # Consume messages
    consume_csv_from_kafka(TOPIC_NAME, GROUP_ID)
