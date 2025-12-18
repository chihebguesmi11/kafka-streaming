"""
JSON Consumer for Kafka Streaming Lab
Reads JSON messages from Kafka, parses them and reports malformed events.
"""

from kafka import KafkaConsumer
import json
import sys


def create_consumer(topic_name, group_id='json-consumer-group', auto_offset_reset='earliest'):
	return KafkaConsumer(
		topic_name,
		bootstrap_servers=['localhost:9092'],
		group_id=group_id,
		auto_offset_reset=auto_offset_reset,
		value_deserializer=lambda v: v.decode('utf-8')
	)


def consume_json(topic_name, group_id='json-consumer-group'):
	consumer = create_consumer(topic_name, group_id)

	print('='*70)
	print(f"👂 JSON Consumer started - Listening to topic '{topic_name}'")
	print(f"👥 Consumer Group: {group_id}")
	print('='*70)
	print('Press Ctrl+C to stop\n')

	count = 0
	try:
		for message in consumer:
			count += 1
			raw = message.value
			try:
				obj = json.loads(raw)
				print(f"{'='*40}")
				print(f"Message #{count}")
				print(f"Partition: {message.partition} | Offset: {message.offset}")
				print(f"Payload: {json.dumps(obj, ensure_ascii=False)}")
			except json.JSONDecodeError:
				print(f"⚠️  Malformed JSON at message #{count}: {raw}")

	except KeyboardInterrupt:
		print('\n' + '='*70)
		print(f"⏹️  Consumer stopped. Total messages consumed: {count}")
		print('='*70)
	except Exception as e:
		print(f"❌ Error in consumer: {e}")
	finally:
		consumer.close()
		print('✓ Consumer closed')


if __name__ == '__main__':
	TOPIC = 'transactions-json'
	GROUP = 'json-consumer-group'
	if len(sys.argv) > 1:
		GROUP = sys.argv[1]
	consume_json(TOPIC, GROUP)

