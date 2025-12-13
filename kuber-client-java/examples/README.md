# Kuber Messaging Test Clients (Java)

These are standalone test clients for testing Kuber's Request/Response messaging feature with different message brokers.

## Important Note

These files are **not compiled** as part of the main build because they require external dependencies that may not be available in all environments (particularly IBM MQ which requires IBM's Maven repository).

## Dependencies Required

To compile and run these examples, add the following dependencies to your project:

### Kafka
```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>3.6.1</version>
</dependency>
```

### ActiveMQ
```xml
<dependency>
    <groupId>org.apache.activemq</groupId>
    <artifactId>activemq-client</artifactId>
    <version>5.18.3</version>
</dependency>
```

### RabbitMQ
```xml
<dependency>
    <groupId>com.rabbitmq</groupId>
    <artifactId>amqp-client</artifactId>
    <version>5.20.0</version>
</dependency>
```

### IBM MQ
```xml
<!-- Requires IBM Maven repository or manual installation -->
<dependency>
    <groupId>com.ibm.mq</groupId>
    <artifactId>com.ibm.mq.allclient</artifactId>
    <version>9.3.4.0</version>
</dependency>
```

For IBM MQ, you may need to add IBM's Maven repository:
```xml
<repository>
    <id>ibm-maven</id>
    <url>https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqdev/maven/</url>
</repository>
```

## How to Use

1. Copy the desired test file to your project's `src/main/java` directory
2. Add the required dependency to your `pom.xml`
3. Update the configuration constants (broker URLs, API keys, etc.)
4. Run the test class

## Test Files

| File | Broker | Protocol |
|------|--------|----------|
| `KafkaRequestResponseTest.java` | Apache Kafka | Native Kafka |
| `ActiveMqRequestResponseTest.java` | Apache ActiveMQ | JMS/OpenWire |
| `RabbitMqRequestResponseTest.java` | RabbitMQ | AMQP |
| `IbmMqRequestResponseTest.java` | IBM MQ | JMS |
| `KuberMessagingExample.java` | Generic messaging | REST API |

## Configuration

Each test file has configuration constants at the top that you need to update:

```java
// Broker connection
private static final String BROKER_URL = "localhost:9092";

// Kuber API key
private static final String API_KEY = "kub_your_api_key_here";

// Queue/Topic names
private static final String REQUEST_QUEUE = "ccs_cache_request";
private static final String RESPONSE_QUEUE = "ccs_cache_response";
```

## Running Tests

```bash
# Compile with dependencies
mvn compile

# Run Kafka test
mvn exec:java -Dexec.mainClass="com.kuber.client.examples.KafkaRequestResponseTest"

# Run ActiveMQ test
mvn exec:java -Dexec.mainClass="com.kuber.client.examples.ActiveMqRequestResponseTest"

# Run RabbitMQ test
mvn exec:java -Dexec.mainClass="com.kuber.client.examples.RabbitMqRequestResponseTest"

# Run IBM MQ test
mvn exec:java -Dexec.mainClass="com.kuber.client.examples.IbmMqRequestResponseTest"
```
