package io.kpininja.connect.cassandra;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BufferedRecords class that handles buffering records efficiently
 * with improved error handling and batch size management.
 */
public class BufferedRecords {
    private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);
    private static final AtomicLong TOTAL_RECORDS_PROCESSED = new AtomicLong(0);

    private final CassandraSinkConfig config;
    private final TableMetadata tableMetadata;
    private final CassandraWriter writer;
    private final List<SinkRecord> records;
    private Schema currentSchema;
    private PreparedStatementBinder preparedStatementBinder;
    private final int maxBatchSize;
    private final RecordValidator validator;
    private final int maxRetries;
    private final int retryBackoffMs;

    public BufferedRecords(CassandraSinkConfig config, TableMetadata tableMetadata, CassandraWriter writer) {
        this.config = config;
        this.tableMetadata = tableMetadata;
        this.writer = writer;
        this.records = new ArrayList<>();
        this.maxBatchSize = config.getBatchSize();
        this.validator = new RecordValidator(tableMetadata, config.getInsertMode());
        this.maxRetries = 3; // Default to 3 retries
        this.retryBackoffMs = 1000; // Default to 1 second backoff
        
        log.debug("Created BufferedRecords for table {}.{} with batch size {}", 
                tableMetadata.getKeyspaceName(), tableMetadata.getTableName(), maxBatchSize);
    }

    /**
     * Add a record to the buffer
     * @param record The SinkRecord to add
     * @return This BufferedRecords instance
     * @throws DataException if record validation fails
     * @throws ConnectException if schema doesn't match
     */
    public BufferedRecords add(SinkRecord record) throws DataException, ConnectException {
        // Validate record before adding
        validator.validate(record);
        
        Schema valueSchema = record.valueSchema();
        
        if (valueSchema != null && !schemaMatches(valueSchema)) {
            // If we have buffered records with a different schema, we need to flush first
            if (!records.isEmpty()) {
                flush();
            }
            
            // Create a new prepared statement binder for this schema
            this.currentSchema = valueSchema;
            this.preparedStatementBinder = createPreparedStatementBinder();
            log.debug("Schema changed for table {}, created new prepared statement binder", 
                    tableMetadata.getTableName());
        }
        
        records.add(record);
        
        if (records.size() >= maxBatchSize) {
            flush();
        }
        
        return this;
    }

    /**
     * Flush all buffered records
     * @throws ConnectException if an error occurs during flush
     * @throws RetriableException for transient errors that may resolve with retry
     */
    public void flush() throws ConnectException, RetriableException {
        if (records.isEmpty()) {
            return;
        }
        
        int recordCount = records.size();
        log.debug("Flushing {} buffered records for table {}", recordCount, tableMetadata.getTableName());
        
        try {
            if (preparedStatementBinder == null) {
                SinkRecord first = records.get(0);
                currentSchema = first.valueSchema();
                preparedStatementBinder = createPreparedStatementBinder();
                log.debug("Created new prepared statement binder during flush");
            }
            
            String insertMode = config.getInsertMode();
            boolean isInsertIfNotExists = CassandraSinkConfig.INSERT_MODE_INSERT_IF_NOT_EXISTS.equals(insertMode);
            
            // For conditional writes (IF NOT EXISTS), process in smaller batches
            if (isInsertIfNotExists) {
                processConditionalWrites();
            } else {
                processStandardWrites();
            }
            
            // Update metrics
            TOTAL_RECORDS_PROCESSED.addAndGet(recordCount);
            
            // Clear the buffer
            records.clear();
            
            // Log progress periodically
            long total = TOTAL_RECORDS_PROCESSED.get();
            if (total % 10000 < recordCount) {
                log.info("Total records flushed to Cassandra: {}", total);
            }
        } catch (Exception e) {
            handleFlushException(e);
        }
    }

    private void processConditionalWrites() {
        // For conditional writes, process in smaller batches to avoid timeouts
        int optimalBatchSize = Math.min(20, maxBatchSize); // Smaller batch size for conditional writes
        List<Object[]> currentBatch = new ArrayList<>(optimalBatchSize);
        
        for (SinkRecord record : records) {
            currentBatch.add(preparedStatementBinder.bindRecord(record));
            
            if (currentBatch.size() >= optimalBatchSize) {
                writer.write(currentBatch, true, tableMetadata.getTableName());
                currentBatch.clear();
            }
        }
        
        // Write any remaining records
        if (!currentBatch.isEmpty()) {
            writer.write(currentBatch, true, tableMetadata.getTableName());
        }
    }

    private void processStandardWrites() {
        // Process standard writes with optimal batch sizing
        int recordCount = records.size();
        
        if (recordCount <= maxBatchSize) {
            // Normal case - all records fit in one batch
            List<Object[]> parametersList = new ArrayList<>(recordCount);
            for (SinkRecord record : records) {
                parametersList.add(preparedStatementBinder.bindRecord(record));
            }
            writer.write(parametersList, false, tableMetadata.getTableName());
        } else {
            // Split into smaller batches if we have a large number of records
            log.debug("Splitting {} records into smaller batches", recordCount);
            List<Object[]> currentBatch = new ArrayList<>(maxBatchSize);
            
            for (SinkRecord record : records) {
                currentBatch.add(preparedStatementBinder.bindRecord(record));
                
                if (currentBatch.size() >= maxBatchSize) {
                    writer.write(currentBatch, false, tableMetadata.getTableName());
                    currentBatch.clear();
                }
            }
            
            // Write any remaining records
            if (!currentBatch.isEmpty()) {
                writer.write(currentBatch, false, tableMetadata.getTableName());
            }
        }
    }

    private void handleFlushException(Exception e) {
        String errorMessage = "Error flushing records to table " + tableMetadata.getTableName();
        log.error("{}: {}", errorMessage, e.getMessage());
        
        // Check if the error is retriable
        if (isRetriableError(e)) {
            log.warn("Encountered retriable error, will be retried by the framework");
            throw new RetriableException(errorMessage, e);
        } else {
            throw new ConnectException(errorMessage, e);
        }
    }

    private boolean isRetriableError(Exception e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }
        
        // Common Cassandra retriable errors
        return message.contains("Operation timed out") ||
               message.contains("Connection reset") ||
               message.contains("Connection refused") ||
               message.contains("No host available") ||
               message.contains("Transport failure") ||
               message.contains("Timeout during") ||
               message.contains("Unavailable exception");
    }

    private boolean schemaMatches(Schema valueSchema) {
        if (currentSchema == null) {
            return false;
        }
        
        return currentSchema.equals(valueSchema);
    }

    private PreparedStatementBinder createPreparedStatementBinder() {
        if (currentSchema == null) {
            throw new ConnectException("Cannot create statement binder without a schema");
        }
        
        validateSchema(currentSchema);

        //validate that the schema matches the table metadata insert.mode is insert if not exists
        
        FieldsMetadata fieldsMetadata = new FieldsMetadata.Builder(config.getInsertMode())
                .addFields(currentSchema, tableMetadata)
                .build();
        
        return new PreparedStatementBinder(fieldsMetadata, tableMetadata);
    }

    private void validateSchema(Schema schema) {
        if (schema.type() != Schema.Type.STRUCT) {
            throw new ConnectException("Records must have a struct schema, but found " + schema.type());
        }
        
        // Verify that all required columns (primary key) are present in the schema
        for (String requiredField : tableMetadata.getPrimaryKeyColumns()) {
            boolean found = false;
            
            // Case-insensitive field lookup
            for (org.apache.kafka.connect.data.Field field : schema.fields()) {
                if (field.name().toLowerCase().equals(requiredField.toLowerCase())) {
                    found = true;
                    break;
                }
            }
            
            if (!found) {
                throw new ConnectException("Schema is missing primary key column: " + requiredField);
            }
        }
    }
}