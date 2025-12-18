"""
JSON Producer for Kafka Streaming Lab
Converts CSV rows into JSON objects and publishes them to Kafka.
"""

from kafka import KafkaProducer
import json
import os
import time


def create_producer():
	return KafkaProducer(
		bootstrap_servers=['localhost:9092'],
		value_serializer=lambda v: json.dumps(v).encode('utf-8')
	)


def csv_row_to_json(header, row):
	fields = [h.strip() for h in header.split(',')]
	values = [v.strip() for v in row.split(',')]
	# Simple mapping: return dict of string values
	return {k: v for k, v in zip(fields, values)}


def stream_csv_as_json(csv_file_path, topic_name, delay=0.1):
	producer = create_producer()

	try:
		with open(csv_file_path, 'r') as f:
			header = f.readline().strip()
			print(f"📊 Header: {header}")

			# Optional: send header as metadata message
			producer.send(topic_name, value={"__header__": header})

			count = 0
			for line in f:
				line = line.strip()
				if not line:
					continue
				payload = csv_row_to_json(header, line)
				producer.send(topic_name, value=payload)
				count += 1
				print(f"✓ Sent JSON #{count}: {payload}")
				time.sleep(delay)

			producer.flush()
			print(f"\n🎉 Streamed {count} JSON messages to '{topic_name}'")

	except FileNotFoundError:
		print(f"❌ CSV file not found: {csv_file_path}")
	except Exception as e:
		print(f"❌ Error producing JSON messages: {e}")
	finally:
		producer.close()


if __name__ == '__main__':
	TOPIC = 'transactions-json'
	CSV_FILE = os.path.join(os.path.dirname(__file__), '..', 'csv_lab', 'data', 'transactions.csv')

	print('='*60)
	print('🚀 Starting JSON Producer')
	print('='*60)
	print(f'📁 CSV source: {CSV_FILE}')
	print(f'📮 Topic: {TOPIC}')
	print('='*60)

	stream_csv_as_json(CSV_FILE, TOPIC, delay=0.05)
