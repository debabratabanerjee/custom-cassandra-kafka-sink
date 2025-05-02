package io.kpininja.connect.cassandra;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(CassandraSinkTask.class);
    
    private CassandraSinkConfig config;
    private CassandraWriter writer;
    private Map<String, BufferedRecords> bufferedRecords = new HashMap<>();

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Cassandra sink task");
        config = new CassandraSinkConfig(props);
        initWriter();
    }

    private void initWriter() {
        try {
            writer = new CassandraWriter(config);
            // No need to initialize buffered records here anymore
            // They will be created dynamically per table
        } catch (Exception e) {
            throw new ConnectException("Failed to create Cassandra writer", e);
        }
    }

    // Helper method to determine table name from topic
    private String getTableForTopic(String topic) {
        // First check explicit mappings
        Map<String, String> topicToTableMap = config.getTopicToTableMap();
        if (topicToTableMap != null && topicToTableMap.containsKey(topic)) {
            return topicToTableMap.get(topic);
        }
        
        // If no explicit mapping, use the topic name
        // Apply transformations if needed (like removing prefixes)
        String[] parts = topic.split("\\.");
        return parts.length > 0 ? parts[parts.length - 1].toLowerCase() : topic.toLowerCase();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        
        log.debug("Received {} records", records.size());
        
        try {
            Map<String, List<SinkRecord>> recordsByTable = new HashMap<>();
            
            // Group records by target table
            for (SinkRecord record : records) {
                String topic = record.topic();
                String tableName = getTableForTopic(topic);
                
                recordsByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(record);
            }
            
            // Process records by table
            for (Map.Entry<String, List<SinkRecord>> entry : recordsByTable.entrySet()) {
                String tableName = entry.getKey();
                List<SinkRecord> tableRecords = entry.getValue();
                
                // Get or create buffered records for this table
                BufferedRecords buffer = bufferedRecords.computeIfAbsent(tableName, k -> {
                    String keyspace = config.getKeyspace();
                    TableMetadata tableMetadata = writer.getTableMetadata(keyspace, tableName);
                    return new BufferedRecords(config, tableMetadata, writer);
                });
                
                // Add records to buffer
                for (SinkRecord record : tableRecords) {
                    buffer.add(record);
                }
            }
        } catch (Exception e) {
            log.error("Error processing records", e);
            throw new ConnectException(e);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.debug("Flushing records");
        try {
            for (BufferedRecords buffer : bufferedRecords.values()) {
                buffer.flush();
            }
        } catch (Exception e) {
            log.error("Error flushing records", e);
            throw new ConnectException(e);
        }
    }

    @Override
    public void stop() {
        log.info("Stopping Cassandra sink task");
        if (bufferedRecords != null && !bufferedRecords.isEmpty()) {
            for (BufferedRecords buffer : bufferedRecords.values()) {
                try {
                    buffer.flush();
                } catch (Exception e) {
                    log.warn("Error flushing records on stop", e);
                }
            }
        }
        
        if (writer != null) {
            try {
                writer.close();
            } catch (Exception e) {
                log.warn("Error closing writer", e);
            }
        }
    }
}