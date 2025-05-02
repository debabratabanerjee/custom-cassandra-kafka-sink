package io.kpininja.connect.cassandra;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Wrapper class for Cassandra table metadata
 */
public class TableMetadata {
    
    private final com.datastax.driver.core.TableMetadata cassandraTableMetadata;
    private final Map<String, ColumnInfo> columnInfo;
    private final List<String> columnNames;
    private final List<String> primaryKeyColumns;
    private final Map<String, String> normalizedColumnNameMap;
    
    public TableMetadata(com.datastax.driver.core.TableMetadata cassandraTableMetadata) {
        this.cassandraTableMetadata = cassandraTableMetadata;
        this.columnInfo = new HashMap<>();
        this.columnNames = new ArrayList<>();
        this.primaryKeyColumns = new ArrayList<>();
        this.normalizedColumnNameMap = new HashMap<>();
        
        for (ColumnMetadata columnMetadata : cassandraTableMetadata.getColumns()) {
            String columnName = columnMetadata.getName();
            DataType dataType = columnMetadata.getType();
            boolean isPrimaryKey = cassandraTableMetadata.getPrimaryKey().contains(columnMetadata);
            
            columnInfo.put(columnName, new ColumnInfo(columnName, dataType, isPrimaryKey));
            columnNames.add(columnName);
            normalizedColumnNameMap.put(columnName.toLowerCase(), columnName);
            
            if (isPrimaryKey) {
                primaryKeyColumns.add(columnName);
            }
        }
    }
    
    /**
     * Get the name of the table
     */
    public String getTableName() {
        return cassandraTableMetadata.getName();
    }
    
    /**
     * Get the name of the keyspace
     */
    public String getKeyspaceName() {
        return cassandraTableMetadata.getKeyspace().getName();
    }
    
    /**
     * Get the list of column names in the table
     */
    public List<String> getColumnNames() {
        return columnNames;
    }
    
    /**
     * Get the list of primary key column names
     */
    public List<String> getPrimaryKeyColumns() {
        List<String> normalizedKeys = new ArrayList<>(primaryKeyColumns.size());
        for (String key : primaryKeyColumns) {
            normalizedKeys.add(key.toLowerCase());  // Always return lowercase
        }
        return normalizedKeys;
    }
    /**
     * Check if a column exists in the table
     */
    public boolean hasColumn(String columnName) {
        return normalizedColumnNameMap.containsKey(columnName.toLowerCase());
    }
    
    /**
     * Get the data type of a column
     */
    public DataType getColumnType(String columnName) {
        String actualColumn = normalizedColumnNameMap.get(columnName.toLowerCase());
        return actualColumn != null ? columnInfo.get(actualColumn).getDataType() : null;
    }
    
    /**
     * Check if a column is part of the primary key
     */
    public boolean isColumnPrimaryKey(String columnName) {
        ColumnInfo column = columnInfo.get(columnName);
        return column != null && column.isPrimaryKey();
    }

    public String getColumnNameByNormalizedName(String name) {
        return normalizedColumnNameMap.get(name.toLowerCase());
    }
    
    /**
     * Inner class to hold column information
     */
    public static class ColumnInfo {
        private final String name;
        private final DataType dataType;
        private final boolean primaryKey;
        
        public ColumnInfo(String name, DataType dataType, boolean primaryKey) {
            this.name = name;
            this.dataType = dataType;
            this.primaryKey = primaryKey;
        }
        
        public String getName() {
            return name;
        }
        
        public DataType getDataType() {
            return dataType;
        }
        
        public boolean isPrimaryKey() {
            return primaryKey;
        }
    }};