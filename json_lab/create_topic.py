"""
Create topic script for json_lab
"""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def create_topic(topic_name, num_partitions=3, replication_factor=1):
    admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'], client_id='json-topic-creator')

    topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"✓ Topic '{topic_name}' created successfully")
    except TopicAlreadyExistsError:
        print(f"⚠️  Topic '{topic_name}' already exists")
    except Exception as e:
        print(f"❌ Error creating topic: {e}")
    finally:
        admin_client.close()


if __name__ == '__main__':
    create_topic('transactions-json', num_partitions=3)
