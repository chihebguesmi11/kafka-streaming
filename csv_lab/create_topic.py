"""
Kafka Topic Creation Script
Creates a topic for CSV streaming with configurable partitions.
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_topic(topic_name, num_partitions=3, replication_factor=1):
    """
    Create a Kafka topic.

    Args:
        topic_name: Name of the topic
        num_partitions: Number of partitions (default: 3)
        replication_factor: Replication factor (default: 1)
    """
    admin_client = KafkaAdminClient(
        bootstrap_servers=['localhost:9092'],
        client_id='topic-creator'
    )

    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor
    )

    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"✓ Topic '{topic_name}' created successfully!")
        print(f"  └─ Partitions: {num_partitions}")
        print(f"  └─ Replication Factor: {replication_factor}")
    except TopicAlreadyExistsError:
        print(f"⚠️  Topic '{topic_name}' already exists")
    except Exception as e:
        print(f"❌ Error creating topic: {e}")
    finally:
        admin_client.close()

def list_topics():
    """List all existing Kafka topics."""
    admin_client = KafkaAdminClient(
        bootstrap_servers=['localhost:9092'],
        client_id='topic-lister'
    )

    try:
        topics = admin_client.list_topics()
        print("\n📋 Existing topics:")
        for topic in topics:
            print(f"  • {topic}")
    except Exception as e:
        print(f"❌ Error listing topics: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    print("=" * 60)
    print("🔧 Kafka Topic Setup")
    print("=" * 60)

    # Create topic for CSV streaming
    TOPIC_NAME = "transactions-csv"
    NUM_PARTITIONS = 3  # Multiple partitions for parallel processing

    create_topic(TOPIC_NAME, NUM_PARTITIONS)
    list_topics()

    print("\n" + "=" * 60)
    print("Topic Design Rationale:")
    print("=" * 60)
    print(f"Topic Name: {TOPIC_NAME}")
    print("  └─ Descriptive name indicating content type (transactions) and format (CSV)")
    print(f"\nPartitions: {NUM_PARTITIONS}")
    print("  └─ Multiple partitions allow parallel processing by consumer groups")
    print("  └─ Enables horizontal scaling of consumers")
    print("  └─ Note: Ordering is only guaranteed within a single partition")
    print("\nReplication Factor: 1")
    print("  └─ Sufficient for development/testing environment")
    print("  └─ Production would use higher replication for fault tolerance")
    print("=" * 60)
