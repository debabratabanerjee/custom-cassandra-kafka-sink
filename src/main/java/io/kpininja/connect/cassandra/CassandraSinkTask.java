package io.kpininja.connect.cassandra;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Production-ready Cassandra sink task with improved batch handling, DLQ support,
 * and robust error handling.
 */
public class CassandraSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(CassandraSinkTask.class);
    
    private CassandraSinkConfig config;
    private CassandraWriter writer;
    private Map<String, BufferedRecords> bufferedRecords = new HashMap<>();
    private ErrantRecordReporter errantRecordReporter;
    private final AtomicInteger recordsProcessed = new AtomicInteger(0);
    private final AtomicInteger batchesProcessed = new AtomicInteger(0);
    
    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Cassandra sink task with properties: {}", maskSensitiveProperties(props));
        config = new CassandraSinkConfig(props);
        
        try {
            // Support for errant record reporting (DLQ)
            errantRecordReporter = context.errantRecordReporter();
            if (errantRecordReporter == null) {
                log.info("Errant record reporter not configured.");
            } else {
                log.info("Errant record reporter available - failed records will be sent to DLQ");
            }
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            log.info("Kafka Connect runtime does not support ErrantRecordReporter, DLQ handling is disabled");
        }

        initWriter();
        log.info("Cassandra sink task started");
    }

    private Map<String, String> maskSensitiveProperties(Map<String, String> props) {
        Map<String, String> maskedProps = new HashMap<>(props);
        if (maskedProps.containsKey(CassandraSinkConfig.CONNECTION_PASSWORD_CONFIG)) {
            maskedProps.put(CassandraSinkConfig.CONNECTION_PASSWORD_CONFIG, "****");
        }
        return maskedProps;
    }

    private void initWriter() {
        try {
            writer = new CassandraWriter(config);
            log.info("Successfully connected to Cassandra cluster");
        } catch (Exception e) {
            throw new ConnectException("Failed to create Cassandra writer: " + e.getMessage(), e);
        }
    }

    // Helper method to determine table name from topic with sanitization
    private String getTableForTopic(String topic) {
        // First check explicit mappings
        Map<String, String> topicToTableMap = config.getTopicToTableMap();
        if (topicToTableMap != null && topicToTableMap.containsKey(topic)) {
            return sanitizeTableName(topicToTableMap.get(topic));
        }
        
        // If no explicit mapping, use the topic name
        // Apply transformations if needed (like removing prefixes)
        String tableName = topic;
        String[] parts = topic.split("\\.");
        if (parts.length > 0) {
            tableName = parts[parts.length - 1];
        }
        
        return sanitizeTableName(tableName);
    }
    
    private String sanitizeTableName(String name) {
        // Replace invalid characters for Cassandra table names
        String sanitized = name.replaceAll("[^a-zA-Z0-9_]", "_");
        
        // Apply case normalization if configured
        if (config.getNormalizeCase()) {
            sanitized = sanitized.toLowerCase();
        }
        
        return sanitized;
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        
        final int recordCount = records.size();
        log.debug("Received {} records", recordCount);
        recordsProcessed.addAndGet(recordCount);
        
        // Group records by target table
        Map<String, List<SinkRecord>> recordsByTable = new HashMap<>();
        for (SinkRecord record : records) {
            String topic = record.topic();
            String tableName = getTableForTopic(topic);
            recordsByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(record);
        }
        
        // Process records by table
        for (Map.Entry<String, List<SinkRecord>> entry : recordsByTable.entrySet()) {
            String tableName = entry.getKey();
            List<SinkRecord> tableRecords = entry.getValue();
            
            try {
                processTableRecords(tableName, tableRecords);
            } catch (Exception e) {
                // If DLQ reporting is available, report failed records and continue
                if (errantRecordReporter != null) {
                    handleRecordErrors(tableRecords, e);
                } else {
                    // Otherwise propagate exception to Kafka Connect
                    log.error("Error processing {} records for table {}: {}", tableRecords.size(), tableName, e.getMessage());
                    if (e instanceof RetriableException) {
                        throw (RetriableException) e;
                    } else {
                        throw new ConnectException("Failed to process records for table " + tableName, e);
                    }
                }
            }
        }
        
        // Log progress periodically (every 10,000 records)
        int current = recordsProcessed.get();
        if (current % 10000 < recordCount) {
            log.info("Total records processed: {}", current);
        }
    }
    
    private void processTableRecords(String tableName, List<SinkRecord> tableRecords) {
        // Get or create buffered records for this table
        BufferedRecords buffer = bufferedRecords.computeIfAbsent(tableName, k -> {
            try {
                String keyspace = config.getKeyspace();
                TableMetadata tableMetadata = writer.getTableMetadata(keyspace, tableName);
                return new BufferedRecords(config, tableMetadata, writer);
            } catch (Exception e) {
                throw new ConnectException("Failed to get table metadata for " + tableName, e);
            }
        });
        
        // Process records in smaller sub-batches if needed
        int batchSize = config.getBatchSize();
        if (tableRecords.size() > batchSize * 2) {
            log.debug("Processing {} records in smaller batches for table {}", tableRecords.size(), tableName);
            
            List<SinkRecord> batch = new ArrayList<>(batchSize);
            for (SinkRecord record : tableRecords) {
                batch.add(record);
                
                if (batch.size() >= batchSize) {
                    processBatch(buffer, batch);
                    batch.clear();
                    batchesProcessed.incrementAndGet();
                }
            }
            
            // Process any remaining records
            if (!batch.isEmpty()) {
                processBatch(buffer, batch);
                batchesProcessed.incrementAndGet();
            }
        } else {
            // Process directly if not too large
            processBatch(buffer, tableRecords);
            batchesProcessed.incrementAndGet();
        }
    }
    
    private void processBatch(BufferedRecords buffer, List<SinkRecord> batch) {
        for (SinkRecord record : batch) {
            try {
                buffer.add(record);
            } catch (DataException e) {
                if (errantRecordReporter != null) {
                    errantRecordReporter.report(record, e);
                    log.warn("Failed to add record, sending to DLQ: {}", e.getMessage());
                } else {
                    throw e;
                }
            }
        }
    }
    
    private void handleRecordErrors(List<SinkRecord> records, Exception exception) {
        for (SinkRecord record : records) {
            try {
                errantRecordReporter.report(record, exception);
                log.warn("Reported record to DLQ: topic={}, partition={}, offset={}", 
                         record.topic(), record.kafkaPartition(), record.kafkaOffset());
            } catch (Exception e) {
                log.error("Failed to report record to DLQ: {}", e.getMessage());
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.debug("Flushing all buffered records, batches processed: {}", batchesProcessed.get());
        
        if (bufferedRecords.isEmpty()) {
            return;
        }
        
        // Track errors to throw after attempting to flush all tables
        Exception firstException = null;
        
        for (Map.Entry<String, BufferedRecords> entry : bufferedRecords.entrySet()) {
            String tableName = entry.getKey();
            BufferedRecords buffer = entry.getValue();
            
            try {
                log.debug("Flushing records for table: {}", tableName);
                buffer.flush();
            } catch (Exception e) {
                log.error("Error flushing records for table {}: {}", tableName, e.getMessage());
                if (firstException == null) {
                    firstException = e;
                }
            }
        }
        
        // If any errors occurred, propagate the first one
        if (firstException != null) {
            if (firstException instanceof RetriableException) {
                throw (RetriableException) firstException;
            } else {
                throw new ConnectException("Error flushing records", firstException);
            }
        }
    }

    @Override
    public void stop() {
        log.info("Stopping Cassandra sink task, total records processed: {}", recordsProcessed.get());
        
        // Attempt to flush any remaining records
        if (!bufferedRecords.isEmpty()) {
            for (BufferedRecords buffer : bufferedRecords.values()) {
                try {
                    buffer.flush();
                } catch (Exception e) {
                    log.warn("Error flushing records on stop: {}", e.getMessage());
                }
            }
        }
        
        // Close resources
        if (writer != null) {
            try {
                writer.close();
                log.info("Cassandra writer closed");
            } catch (Exception e) {
                log.warn("Error closing writer: {}", e.getMessage());
            }
        }
    }
}