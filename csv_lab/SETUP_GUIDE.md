# CSV Lab - Setup and Execution Guide

This guide walks you through completing the CSV streaming lab tasks.

---

## Prerequisites

1. **Docker** installed and running
2. **Python 3.9+** installed
3. **Python dependencies** installed

---

## Step-by-Step Instructions

### Task 1: Kafka Setup

#### 1.1 Start Kafka using Docker

```bash
# From the root directory (Kafka-Streaming/)
docker-compose up -d
```

#### 1.2 Verify Kafka is running

```bash
# Check running containers
docker ps

# You should see two containers:
# - zookeeper
# - kafka
```

#### 1.3 Check Kafka logs (optional)

```bash
docker logs kafka
```

---

### Task 2: Topic Design

#### 2.1 Install Python dependencies

```bash
pip install -r requirements.txt
```

#### 2.2 Create the Kafka topic

```bash
cd csv_lab
python create_topic.py
```

**Topic Design Choices:**
- **Topic Name**: `transactions-csv`
  - Descriptive name indicating content (transactions) and format (CSV)
- **Partitions**: 3
  - Enables parallel processing by multiple consumers
  - Allows horizontal scaling
  - Note: Ordering is only guaranteed within a single partition
- **Replication Factor**: 1
  - Sufficient for development/testing
  - Production environments should use higher values (e.g., 3) for fault tolerance

---

### Task 3: CSV Producer

#### 3.1 Run the Producer

```bash
# Make sure you're in the csv_lab directory
python producer_csv.py
```

**What it does:**
- Reads `data/transactions.csv` line by line
- Sends each row as a separate Kafka message
- Includes header as first message
- Streams gradually with 0.1s delay to simulate real-time data

**Expected output:**
```
🚀 Starting CSV Producer
📁 CSV File: data/transactions.csv
📮 Topic: transactions-csv
✓ Sent header to topic 'transactions-csv'
✓ Sent row 1: 1,103,171.07,2024-01-01 10:00:20
✓ Sent row 2: 2,104,337.1,2024-01-01 10:00:26
...
```

---

### Task 4: CSV Consumer

#### 4.1 Run the Consumer (in a new terminal)

```bash
cd csv_lab
python consumer_csv.py
```

**What it does:**
- Connects to Kafka topic `transactions-csv`
- Reads messages from the beginning (`auto_offset_reset='earliest'`)
- Displays each message with partition, offset, and timestamp
- Tracks message consumption

**Expected output:**
```
👂 Consumer started - Listening to topic 'transactions-csv'
👥 Consumer Group: csv-consumer-group
📩 Message #1
├─ Partition: 0
├─ Offset: 0
├─ Timestamp: 1734393600000
└─ Value: transaction_id,user_id,amount,timestamp
...
```

#### 4.2 Stop the consumer

Press `Ctrl+C` to gracefully stop the consumer.

---

### Task 5: Offsets & Replay

#### 5.1 Restart the Consumer

```bash
python consumer_csv.py
```

**Observation**: Consumer does NOT replay messages because it uses the same consumer group and Kafka remembers the last committed offset.

#### 5.2 Change Consumer Group

```bash
# Use a different consumer group
python consumer_csv.py new-consumer-group
```

**Observation**: Messages are replayed from the beginning because the new consumer group has no previous offset.

**Key Learning:**
- Offsets are tracked per consumer group
- Same group = resume from last position
- Different group = start fresh

---

### Task 6: Consumer Groups

#### 6.1 Run Multiple Consumers in the Same Group

Open **3 terminals** and run:

```bash
# Terminal 1
python consumer_csv.py parallel-group

# Terminal 2
python consumer_csv.py parallel-group

# Terminal 3
python consumer_csv.py parallel-group
```

#### 6.2 Run the Producer Again

```bash
# In a new terminal
python producer_csv.py
```

**Observation:**
- Messages are distributed among the 3 consumers
- Each consumer processes messages from different partitions
- With 3 partitions and 3 consumers, each consumer gets 1 partition

#### 6.3 Increase Partitions (Advanced)

```bash
# Note: You cannot reduce partitions, only increase them
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --alter --topic transactions-csv --partitions 6
```

**Observation with 6 partitions and 3 consumers:**
- Each consumer will handle 2 partitions
- Better load distribution

---

### Task 7: Ordering Guarantees

#### 7.1 Single Partition Test

Create a topic with 1 partition:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic test-ordering --partitions 1 --replication-factor 1
```

Modify `producer_csv.py` to use `test-ordering` topic and run it.

**Observation**: Messages arrive in the exact order they were sent.

#### 7.2 Multiple Partition Test

Use the existing `transactions-csv` topic (3 partitions).

**Observation**:
- Messages may NOT arrive in the same order they were sent
- Order is only guaranteed within a single partition
- Different messages go to different partitions (unless you specify a key)

**Key Learning:**
- Kafka guarantees ordering per partition, not across partitions
- To maintain order for related messages, use the same partition key

---

## Useful Commands

### Check Topics
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Describe Topic
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic transactions-csv
```

### Check Consumer Groups
```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

### Describe Consumer Group
```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group csv-consumer-group
```

### Delete Topic (if needed)
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic transactions-csv
```

### Stop Kafka
```bash
# From root directory
docker-compose down
```

---

## Reflection Questions (Answers)

### 1. Why does Kafka not care about CSV structure?

Kafka is a message broker that treats all data as bytes. It doesn't parse or interpret the content - it only stores and delivers byte arrays. The responsibility of parsing CSV lies with the producer (when serializing) and consumer (when deserializing).

### 2. What problems arise from CSV in streaming systems?

- **No schema enforcement**: Easy to send malformed data
- **Type ambiguity**: All values are strings, requiring manual parsing
- **Evolution challenges**: Adding/removing columns breaks consumers
- **No metadata**: Cannot validate structure without parsing
- **Large messages**: Entire row must be sent as one message
- **Header handling**: First row is special, needs special treatment

### 3. Why are offsets critical?

- **Progress tracking**: Kafka tracks which messages each consumer has processed
- **Fault tolerance**: If a consumer crashes, it can resume from last committed offset
- **Replay capability**: Can reprocess messages by resetting offsets
- **At-least-once delivery**: Ensures messages aren't lost
- **Consumer coordination**: Multiple consumers in a group use offsets to avoid duplicate processing

---

## Next Steps

After completing this lab with clean data:
1. Proceed to `json_lab` for structured data streaming
2. Return to this lab with dirty data (`transactions_dirty.csv`) to handle data quality issues

---

## Troubleshooting

### Port already in use
```bash
# Kill process using port 9092
# Windows:
netstat -ano | findstr :9092
taskkill /PID <PID> /F

# Linux/Mac:
lsof -ti:9092 | xargs kill -9
```

### Cannot connect to Kafka
- Ensure Docker containers are running: `docker ps`
- Check Kafka logs: `docker logs kafka`
- Wait 30 seconds after starting Kafka for it to be fully ready

### Consumer not receiving messages
- Verify topic exists: `docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list`
- Check producer ran successfully
- Verify consumer group offset: Use describe consumer group command above
