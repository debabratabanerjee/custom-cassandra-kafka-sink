package io.kpininja.connect.cassandra;

import com.datastax.driver.core.DataType;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * PreparedStatementBinder maps Connect Schema types to Cassandra types and binds values to prepared statements
 */
public class PreparedStatementBinder {
    private static final Logger log = LoggerFactory.getLogger(PreparedStatementBinder.class);
    
    private final FieldsMetadata fieldsMetadata;
    private final TableMetadata tableMetadata;
    
    public PreparedStatementBinder(FieldsMetadata fieldsMetadata, TableMetadata tableMetadata) {
        this.fieldsMetadata = fieldsMetadata;
        this.tableMetadata = tableMetadata;
    }
    
    /**
     * Bind a sink record to a statement
     * @param record The sink record to bind
     * @return Array of parameters for the prepared statement
     */
    public Object[] bindRecord(SinkRecord record) {
        if (record.value() == null) {
            throw new ConnectException("Record value cannot be null");
        }
        
        Struct recordValue = (Struct) record.value();
        
        List<String> columnNames = tableMetadata.getColumnNames();
        Object[] parameters = new Object[columnNames.size()];
        
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            Field field = fieldsMetadata.getFieldByName(columnName);
            
            if (field == null) {
                parameters[i] = null;
                continue;
            }
            
            Object fieldValue = extractValue(recordValue, field);
            DataType cassandraType = tableMetadata.getColumnType(columnName);
            
            parameters[i] = convertToCassandraType(fieldValue, field.schema(), cassandraType);
        }
        
        return parameters;
    }
    
    /**
     * Extract a value from a Struct
     */
    private Object extractValue(Struct struct, Field field) {
        return struct.get(field);
    }
    
    /**
     * Convert Connect Schema value to Cassandra type
     */
    private Object convertToCassandraType(Object value, Schema schema, DataType cassandraType) {
        if (value == null) {
            return null;
        }
        
        switch (schema.type()) {
            case INT8:
            case INT16:
            case INT32:
                // Handle integer types
                if (cassandraType == DataType.smallint()) {
                    return ((Number) value).shortValue();
                } else if (cassandraType == DataType.tinyint()) {
                    return ((Number) value).byteValue();
                } else {
                    return ((Number) value).intValue();
                }
                
            case INT64:
                return ((Number) value).longValue();
                
            case FLOAT32:
                return ((Number) value).floatValue();
                
            case FLOAT64:
                return ((Number) value).doubleValue();
                
            case BOOLEAN:
                return value;
                
            case STRING:
                String strValue = (String) value;
                // Handle UUID types
                if (cassandraType == DataType.uuid()) {
                    return UUID.fromString(strValue);
                } else if (cassandraType == DataType.timeuuid()) {
                    return UUID.fromString(strValue);
                }
                return strValue;
                
            case BYTES:
                byte[] bytes = (byte[]) value;
                return ByteBuffer.wrap(bytes);
                
            case ARRAY:
                return value; // Cassandra driver handles Java Lists
                
            case MAP:
                return value; // Cassandra driver handles Java Maps
                
                case STRUCT:
                // Convert structs based on logical type if present
                if (schema.name() != null) {
                    switch (schema.name()) {
                        case org.apache.kafka.connect.data.Date.LOGICAL_NAME:
                            int days = (Integer) value;
                            long millis = days * 86_400_000L;
                            return new Date(millis);
            
                        case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                            return new Date(((java.util.Date) value).getTime());
            
                        case org.apache.kafka.connect.data.Decimal.LOGICAL_NAME:
                            return (BigDecimal) value;
            
                        default:
                            // For other struct types, try to handle as Cassandra UDT (not implemented)
                            log.warn("Complex type conversion not fully implemented for {}", schema.name());
                            return null;
                    }
                }
                log.warn("Unsupported struct type without logical type: {}", schema);
                return null;
            
                
            default:
                log.warn("Unsupported schema type: {}", schema.type());
                return null;
        }
    }
}