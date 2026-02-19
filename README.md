# Storm Word Count Pipeline

A real-time word counting application built using Apache Storm that consumes JSON messages from Apache Kafka, splits text into words, and counts their occurrences.

## Project Description

This project demonstrates a distributed stream processing pipeline using Apache Storm and Kafka. The topology consists of:

- **Kafka Spout**: Consumes JSON messages from a Kafka topic
- **Split Bolt**: Parses JSON and splits text into individual words
- **Count Bolt**: Maintains running counts of word occurrences

The application processes messages in the format:
```json
{
  "text": "your paragraph text here"
}
```

## Architecture

```
Kafka Topic (word-input)
    ↓
Kafka Spout
    ↓
Split Bolt (parallelism: 2)
    ↓
Count Bolt (parallelism: 2)
```

## Prerequisites

Before running this application, ensure you have the following installed:

- **Java 8** or higher
- **Apache Maven 3.6+**
- **Apache Kafka 2.8.1** (or compatible version)
- **Apache ZooKeeper** (required for Kafka)

## Project Structure

```
storm-word-count/
├── src/
│   └── main/
│       ├── java/
│       │   └── wordcount/
│       │       ├── topology/
│       │       │   ├── WordCountTopology.java    # Main topology
│       │       │   └── bolts/
│       │       │       ├── SplitBolt.java        # Text splitting bolt
│       │       │       └── CountBolt.java        # Word counting bolt
│       │       └── util/
│       │           ├── ConfigLoader.java         # Configuration loader
│       │           └── KafkaConfigFactory.java   # Kafka config factory
│       └── resources/
│           └── config.properties                 # Application configuration
├── pom.xml
└── README.md
```

## Configuration

The application uses `config.properties` for configuration:

```properties
# Kafka Settings
kafka.bootstrap.servers=127.0.0.1:9092
kafka.topic.input=word-input
kafka.group.id=storm-wordcount-group

# Topology Settings
topology.parallelism.splitter=2
topology.parallelism.counter=2
```

You can modify these settings based on your Kafka setup.

## Setup Instructions

### Step 1: Start ZooKeeper

ZooKeeper is required for Kafka to run.

```bash
# Start ZooKeeper (default port: 2181)
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### Step 2: Start Kafka

```bash
# Start Kafka broker (default port: 9092)
bin/kafka-server-start.sh config/server.properties
```

### Step 3: Create Kafka Topic

Create the input topic that the Storm topology will consume from:

```bash
# Create topic 'word-input'
bin/kafka-topics.sh --create \
  --topic word-input \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Verify the topic was created:

```bash
# List all topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Step 4: Build the Application

Navigate to the project directory and build the JAR:

```bash
cd /Users/aditya.hansraj/code/storm-word-count
mvn clean package
```

This will create `storm-word-count-1.0-SNAPSHOT.jar` in the `target/` directory.

### Step 5: Run the Storm Topology

Execute the JAR file to start the Storm topology in local mode:

```bash
java -jar target/storm-word-count-1.0-SNAPSHOT.jar
```

The topology will run for 10 minutes (600 seconds) by default.

### Step 6: Produce Test Messages to Kafka

In a new terminal, produce some test messages to Kafka:

```bash
# Start Kafka console producer
bin/kafka-console-producer.sh \
  --topic word-input \
  --bootstrap-server localhost:9092
```

Then send JSON messages (one per line):

```json
{"text": "hello world"}
{"text": "hello storm and kafka"}
{"text": "real time stream processing with apache storm"}
```

## Expected Output

As messages are consumed and processed, you'll see output similar to:

```
Result -> Word: [hello] | Count: 1
Result -> Word: [world] | Count: 1
Result -> Word: [hello] | Count: 2
Result -> Word: [storm] | Count: 1
Result -> Word: [and] | Count: 1
Result -> Word: [kafka] | Count: 1
Result -> Word: [real] | Count: 1
Result -> Word: [time] | Count: 1
Result -> Word: [stream] | Count: 1
Result -> Word: [processing] | Count: 1
Result -> Word: [with] | Count: 1
Result -> Word: [apache] | Count: 1
Result -> Word: [storm] | Count: 2
```

## Troubleshooting

### Issue: InvalidGroupIdException

**Error**: `To use the group management or offset commit APIs, you must provide a valid group.id`

**Solution**: Ensure `kafka.group.id` is properly set in `config.properties` and being passed to the Kafka configuration.

### Issue: Connection Refused to Kafka

**Error**: `Connection to node -1 could not be established`

**Solution**: 
- Verify Kafka is running on the configured host and port
- Check `kafka.bootstrap.servers` in `config.properties`
- Ensure ZooKeeper is running before starting Kafka

### Issue: Topic Does Not Exist

**Error**: `Unknown topic or partition`

**Solution**: Create the topic using the command in Step 3 above.

### Issue: Compilation Errors

**Error**: Method signature mismatch in `prepare()` method

**Solution**: Ensure the `prepare()` method uses raw `Map` type instead of `Map<String, Object>`:
```java
public void prepare(Map topoConf, TopologyContext context, OutputCollector collector)
```

## Development

### Running Tests

```bash
mvn test
```

### Cleaning Build Artifacts

```bash
mvn clean
```

### Modifying Parallelism

Edit `config.properties` to change parallelism settings:

```properties
topology.parallelism.splitter=4
topology.parallelism.counter=4
```

## Dependencies

- Apache Storm 1.2.4
- Apache Kafka Client 2.8.1
- Jackson Databind 2.13.5

See `pom.xml` for complete dependency list.

## Notes

- The application runs in **local cluster mode** for development and testing
- By default, the topology runs for **10 minutes** before shutting down
- Word counting is **case-insensitive** (all words converted to lowercase)
- Only alphanumeric characters are considered; punctuation is removed
- Empty words are filtered out

## License

This project is for educational and demonstration purposes.

## Author

Built with Apache Storm and Kafka for real-time stream processing.

