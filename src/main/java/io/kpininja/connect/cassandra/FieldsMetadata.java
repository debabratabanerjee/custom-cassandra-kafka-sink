package io.kpininja.connect.cassandra;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * FieldsMetadata maps between Connect Schema fields and Cassandra table columns
 */
public class FieldsMetadata {
    private static final Logger log = LoggerFactory.getLogger(FieldsMetadata.class);
    
    private final Map<String, Field> fieldsByName;
    
    private FieldsMetadata(Map<String, Field> fieldsByName) {
        this.fieldsByName = fieldsByName;
    }
    
    /**
     * Get a field by column name
     */
    public Field getFieldByName(String columnName) {
        // First try exact match
        Field field = fieldsByName.get(columnName);
        
        // If not found, try case-insensitive match
        if (field == null) {
            String lowerColumnName = columnName.toLowerCase();
            for (Map.Entry<String, Field> entry : fieldsByName.entrySet()) {
                if (entry.getKey().toLowerCase().equals(lowerColumnName)) {
                    return entry.getValue();
                }
            }
        }
        
        return field;
    }
    
    public static class Builder {
        private final Map<String, Field> fieldsByName = new HashMap<>();
        private final boolean validatePkFields;
        
        /**
         * Add fields from a Connect Schema
         */


         public Builder(String insertMode) {
            // Only validate PK fields for INSERT_IF_NOT_EXISTS mode
            this.validatePkFields = CassandraSinkConfig.INSERT_MODE_INSERT_IF_NOT_EXISTS.equals(insertMode);
        }

        public Builder addFields(Schema schema, TableMetadata tableMetadata) {
            if (schema.type() != Schema.Type.STRUCT) {
                throw new ConnectException("Only Struct schemas are supported");
            }
            
            // Pre-process fields for efficient lookup
            for (Field field : schema.fields()) {
                String normalizedName = field.name().toLowerCase();
                String actualColumnName = tableMetadata.getColumnNameByNormalizedName(normalizedName);
                
                if (actualColumnName != null) {
                    fieldsByName.put(actualColumnName, field);
                } else {
                    log.warn("Field {} in schema has no corresponding column in table {}.{}", 
                            field.name(), tableMetadata.getKeyspaceName(), tableMetadata.getTableName());
                }
            }
            
            // Check primary keys only if validation is required
        if (validatePkFields) {
            for (String pkColumn : tableMetadata.getPrimaryKeyColumns()) {
                boolean found = false;
                for (String fieldName : fieldsByName.keySet()) {
                    if (fieldName.equalsIgnoreCase(pkColumn)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new ConnectException("Schema is missing primary key column: " + pkColumn);
                }
            }
        }
        
        return this;
    }
        
        /**
         * Build the FieldsMetadata
         */
        public FieldsMetadata build() {
            return new FieldsMetadata(fieldsByName);
        }
    }
}