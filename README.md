# Kafka Connect Cassandra Sink Connector

A production-ready Kafka Connect sink connector for Apache Cassandra that efficiently copies data from Kafka to Cassandra.

## Features

- High-performance batched writes to Cassandra
- Support for multiple insertion modes (`INSERT`, `INSERT_IF_NOT_EXISTS`)
- Configurable consistency levels
- Flexible topic-to-table mapping
- Robust error handling with DLQ (Dead Letter Queue) support
- Support for TTL (Time-To-Live) settings
- Case normalization and identifier quoting options
- Advanced connection pool management
- Comprehensive logging

## Getting Started

### Prerequisites

- Apache Kafka (3.x or later)
- Apache Cassandra (3.x or later)
- Java 11 or later

### Building the Connector

```bash
mvn clean package
```

This will create two artifacts in the `target` directory:
- `kafka-connect-cassandra-1.0.0.jar` - The connector JAR
- `kafka-connect-cassandra-1.0.0-package.tar.gz` - A packaged version with all dependencies

### Installing the Connector

Copy the packaged connector to your Kafka Connect plugins directory:

```bash
mkdir -p /usr/local/share/kafka/plugins/
tar -xzf target/kafka-connect-cassandra-1.0.0-package.tar.gz -C /usr/local/share/kafka/plugins/
```

## Configuration

### Required Configuration Properties

| Name | Description | Type | Default | Importance |
|------|-------------|------|---------|------------|
| `connection.url` | Cassandra connection URL in format: `cassandra://host1,host2:9042` | string | - | high |
| `keyspace` | The Cassandra keyspace to write to | string | - | high |
| `pk.fields` | Comma-separated list of field names that are part of the primary key | string | - | high |

### Connection Properties

| Name | Description | Type | Default | Importance |
|------|-------------|------|---------|------------|
| `connection.user` | Cassandra connection username for authentication | string | null | medium |
| `connection.password` | Cassandra connection password for authentication | password | null | medium |
| `max.connections.per.host` | Maximum number of connections per host | int | 8 | medium |
| `connection.timeout.ms` | Connection timeout in milliseconds | int | 5000 | medium |

### Target Properties

| Name | Description | Type | Default | Importance |
|------|-------------|------|---------|------------|
| `table` | The default Cassandra table (if not specified by topic mapping) | string | "" | medium |
| `topic.table.map` | Comma-separated mappings in the form 'topic:table' | string | "" | medium |

### Write Behavior

| Name | Description | Type | Default | Importance |
|------|-------------|------|---------|------------|
| `batch.size` | Number of records to batch together | int | 100 | medium |
| `insert.mode` | Insertion mode: `INSERT` or `INSERT_IF_NOT_EXISTS` | string | INSERT | high |
| `consistency.level` | Consistency level for writes | string | LOCAL_QUORUM | medium |
| `ttl.seconds` | Time-to-live in seconds (0 = no TTL) | int | 0 | low |

### Formatting Options

| Name | Description | Type | Default | Importance |
|------|-------------|------|---------|------------|
| `quote.identifiers` | Whether to quote identifiers in CQL | boolean | false | medium |
| `normalize.case` | Convert identifiers to lowercase | boolean | true | high |

### Error Handling

| Name | Description | Type | Default | Importance |
|------|-------------|------|---------|------------|
| `max.retries` | Maximum number of retries for retriable errors | int | 10 | medium |
| `retry.backoff.ms` | Time to wait between retries | int | 3000 | medium |

## Example Configuration

```json
{
  "name": "cassandra-sink",
  "config": {
    "connector.class": "io.kpininja.connect.cassandra.CassandraSinkConnector",
    "tasks.max": "3",
    "topics": "my_topic,another_topic",
    "connection.url": "cassandra://cassandra1.example.com,cassandra2.example.com:9042",
    "connection.user": "username",
    "connection.password": "password",
    "keyspace": "mykeyspace",
    "topic.table.map": "my_topic:my_table,another_topic:another_table",
    "pk.fields": "id",
    "batch.size": "100",
    "insert.mode": "INSERT",
    "consistency.level": "LOCAL_QUORUM",
    "ttl.seconds": "86400",
    "normalize.case": "true",
    "errors.tolerance": "all",
    "errors.deadletterqueue.topic.name": "dlq",
    "errors.deadletterqueue.context.headers.enable": "true"
  }
}
```

## Topic to Table Mapping

By default, the connector will use the Kafka topic name as the Cassandra table name. You can override this behavior by:

1. Setting a default table name with the `table` config property
2. Providing explicit mappings with the `topic.table.map` property

For example, with:
```
topic.table.map=orders:customer_orders,shipments:logistics_shipments
```

Records from the `orders` topic will be written to the `customer_orders` table, and records from the `shipments` topic will be written to the `logistics_shipments` table.

## Schema Handling

The connector requires that incoming records have schemas. Fields from the record schema will be matched to columns in the Cassandra table, with the following features:

- Case-insensitive field matching (controlled by `normalize.case`)
- Validation of primary key fields (from `pk.fields` configuration)
- Data type conversion between Connect schema types and Cassandra types

## Performance Tuning

For optimal performance:

1. Set an appropriate `batch.size` (100-500 is generally good)
2. Configure `tasks.max` according to your Kafka topic partition count
3. Use a consistent schema for your records
4. Avoid using `INSERT_IF_NOT_EXISTS` for high-throughput scenarios
5. Consider proper sizing of connection pool with `max.connections.per.host`

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify connection.url, credentials, and Cassandra cluster status
2. **Schema Errors**: Ensure record schemas match Cassandra table schema
3. **Batching Issues**: Reduce batch.size if seeing timeout errors
4. **Missing Tables**: Verify tables exist in the keyspace before starting connector
5. **Slow Performance**: Check batch.size, connection pool settings, and Cassandra cluster performance

### Enabling Detailed Logging

Add the following to your Connect worker's log4j configuration:

```properties
log4j.logger.io.kpininja.connect.cassandra=DEBUG
```

## License

[Apache License 2.0](LICENSE)