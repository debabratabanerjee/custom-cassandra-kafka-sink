package io.kpininja.connect.cassandra;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Validates SinkRecords before writing to Cassandra
 */
public class RecordValidator {
    private static final Logger log = LoggerFactory.getLogger(RecordValidator.class);
    
    private final TableMetadata tableMetadata;
    
    public RecordValidator(TableMetadata tableMetadata) {
        this.tableMetadata = tableMetadata;
    }
    
    /**
     * Validate that a record can be written to Cassandra
     * @param record The record to validate
     * @throws DataException if validation fails
     */
    public void validate(SinkRecord record) {
        if (record.value() == null) {
            throw new DataException("Record value cannot be null");
        }
        
        Schema schema = record.valueSchema();
        if (schema == null) {
            throw new DataException("Record schema cannot be null");
        }
        
        if (schema.type() != Schema.Type.STRUCT) {
            throw new DataException("Record must have a struct schema");
        }
        
        // Ensure the record is a Struct
        if (!(record.value() instanceof Struct)) {
            throw new DataException("Record value must be a Struct");
        }
        
        Struct value = (Struct) record.value();
        
        // Check that all primary key columns exist in the record with non-null values
        List<String> pkColumns = tableMetadata.getPrimaryKeyColumns();
        for (String pkColumn : pkColumns) {
            Field field = schema.field(pkColumn);
            if (field == null) {
                throw new DataException("Primary key column " + pkColumn + " missing from record schema");
            }
            
            Object fieldValue = value.get(field);
            if (fieldValue == null) {
                throw new DataException("Primary key column " + pkColumn + " cannot be null");
            }
        }
    }
}