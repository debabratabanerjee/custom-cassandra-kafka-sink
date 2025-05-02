package io.kpininja.connect.cassandra;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.UserType.Field;

import java.util.ArrayList;
import java.util.List;

/**
 * BufferedRecords class that handles buffering records and creating prepared CQL statements
 */
public class BufferedRecords {
    private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

    private final CassandraSinkConfig config;
    private final TableMetadata tableMetadata;
    private final CassandraWriter writer;
    private final List<SinkRecord> records;
    private Schema currentSchema;
    private PreparedStatementBinder preparedStatementBinder;

    public BufferedRecords(CassandraSinkConfig config, TableMetadata tableMetadata, CassandraWriter writer) {
        this.config = config;
        this.tableMetadata = tableMetadata;
        this.writer = writer;
        this.records = new ArrayList<>();
    }

    /**
     * Add a record to the buffer
     * @param record The SinkRecord to add
     * @return This BufferedRecords instance
     * @throws ConnectException if record schema doesn't match current schema
     */
    public BufferedRecords add(SinkRecord record) throws ConnectException {
        Schema valueSchema = record.valueSchema();
        
        if (valueSchema != null && !schemaMatches(valueSchema)) {
            // If we have buffered records with a different schema, we need to flush first
            if (!records.isEmpty()) {
                flush();
            }
            
            // Create a new prepared statement binder for this schema
            this.currentSchema = valueSchema;
            this.preparedStatementBinder = createPreparedStatementBinder();
        }
        
        records.add(record);
        
        if (records.size() >= config.getBatchSize()) {
            flush();
        }
        
        return this;
    }

    /**
     * Flush all buffered records
     * @throws ConnectException if an error occurs during flush
     */
    public void flush() throws ConnectException {
        if (records.isEmpty()) {
            return;
        }
        
        try {
            log.debug("Flushing {} buffered records", records.size());
            
            if (preparedStatementBinder == null) {
                SinkRecord first = records.get(0);
                currentSchema = first.valueSchema();
                preparedStatementBinder = createPreparedStatementBinder();
            }
            
            String insertMode = config.getInsertMode();
            boolean isInsertIfNotExists = CassandraSinkConfig.INSERT_MODE_INSERT_IF_NOT_EXISTS.equals(insertMode);
            
            // For conditional writes (IF NOT EXISTS), process each record individually
            if (isInsertIfNotExists) {
                for (SinkRecord record : records) {
                    List<Object[]> singleRecordBatch = new ArrayList<>(1);
                    singleRecordBatch.add(preparedStatementBinder.bindRecord(record));
                    writer.write(singleRecordBatch, isInsertIfNotExists, tableMetadata.getTableName());
                }
            } else {
                // Original batching logic for non-conditional writes
                List<Object[]> parametersList = new ArrayList<>(records.size());
                for (SinkRecord record : records) {
                    parametersList.add(preparedStatementBinder.bindRecord(record));
                }
                writer.write(parametersList, isInsertIfNotExists, tableMetadata.getTableName());
            }
            
            // Clear the buffer
            records.clear();
        } catch (Exception e) {
            throw new ConnectException("Error flushing records", e);
        }
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
        
        FieldsMetadata fieldsMetadata = new FieldsMetadata.Builder()
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