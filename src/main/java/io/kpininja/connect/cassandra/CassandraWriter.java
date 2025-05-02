package io.kpininja.connect.cassandra;

import com.datastax.driver.core.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CassandraWriter handles the connection to Cassandra and writes records
 */
public class CassandraWriter implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(CassandraWriter.class);

    private final CassandraSinkConfig config;
    private final Cluster cluster;
    private final Session session;
    private final ConsistencyLevel consistencyLevel;
    private final Map<String, PreparedStatement> preparedStatementCache;
    private final int ttlSeconds;

    public CassandraWriter(CassandraSinkConfig config) {
        this.config = config;
        this.preparedStatementCache = new HashMap<>();
        this.ttlSeconds = config.getTtl();
        
        // Parse consistency level
        try {
            this.consistencyLevel = ConsistencyLevel.valueOf(config.getConsistencyLevel());
        } catch (IllegalArgumentException e) {
            throw new ConnectException("Invalid consistency level: " + config.getConsistencyLevel(), e);
        }
        
        // Initialize Cassandra cluster and session
        Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPoints(parseContactPoints(config.getConnectionUrl()))
                .withPort(parsePort(config.getConnectionUrl()));
        
        // Add authentication if provided
        if (config.getConnectionUser() != null && !config.getConnectionUser().isEmpty()) {
            clusterBuilder.withCredentials(config.getConnectionUser(), config.getConnectionPassword());
        }
        
        this.cluster = clusterBuilder.build();
        this.session = cluster.connect();
        
        log.info("Connected to Cassandra cluster: {}", cluster.getMetadata().getClusterName());
    }

    /**
     * Get table metadata for a given keyspace and table
     */
    public TableMetadata getTableMetadata(String keyspace, String tableName) {
        KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace);
        if (keyspaceMetadata == null) {
            throw new ConnectException("Keyspace " + keyspace + " does not exist");
        }
        
        com.datastax.driver.core.TableMetadata cassandraTableMetadata = keyspaceMetadata.getTable(tableName);
        if (cassandraTableMetadata == null) {
            throw new ConnectException("Table " + keyspace + "." + tableName + " does not exist");
        }
        
        return new TableMetadata(cassandraTableMetadata);
    }

    /**
     * Write a batch of records to Cassandra
     */
    public void write(List<Object[]> parametersList, boolean isInsertIfNotExists, String tableName) {
        if (parametersList.isEmpty()) {
            return;
        }
        
        // Get or create prepared statement
        PreparedStatement preparedStatement = getPreparedStatement(isInsertIfNotExists, tableName);
        
        // Create batch statement if we have multiple records
        if (parametersList.size() > 1) {
            BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
            batchStatement.setConsistencyLevel(consistencyLevel);
            
            for (Object[] parameters : parametersList) {
                BoundStatement boundStatement = preparedStatement.bind(parameters);
                batchStatement.add(boundStatement);
            }
            
            try {
                session.execute(batchStatement);
            } catch (Exception e) {
                throw new ConnectException("Error executing batch statement", e);
            }
        } else {
            // Single record, execute directly
            BoundStatement boundStatement = preparedStatement.bind(parametersList.get(0));
            boundStatement.setConsistencyLevel(consistencyLevel);
            
            try {
                session.execute(boundStatement);
            } catch (Exception e) {
                throw new ConnectException("Error executing statement", e);
            }
        }
    }

    /**
     * Get or create a prepared statement for the current configuration
     */
    private PreparedStatement getPreparedStatement(boolean isInsertIfNotExists, String tableName) {
        String cacheKey = getCacheKey(isInsertIfNotExists, tableName);
        
        PreparedStatement preparedStatement = preparedStatementCache.get(cacheKey);
        if (preparedStatement == null) {
            preparedStatement = createPreparedStatement(isInsertIfNotExists, tableName);
            preparedStatementCache.put(cacheKey, preparedStatement);
        }
        
        return preparedStatement;
    }

    /**
     * Create a prepared statement for the current configuration
     */
    private PreparedStatement createPreparedStatement(boolean isInsertIfNotExists, String tableName) {
        String keyspace = config.getKeyspace();
        boolean quoteIdentifiers = config.getQuoteIdentifiers();
        
        TableMetadata tableMetadata = getTableMetadata(keyspace, tableName);
        List<String> columnNames = tableMetadata.getColumnNames();
        
        // Build the CQL statement
        StringBuilder cqlBuilder = new StringBuilder();
        cqlBuilder.append("INSERT INTO ");
        
        if (quoteIdentifiers) {
            cqlBuilder.append(quoteIdentifier(keyspace)).append(".").append(quoteIdentifier(tableName));
        } else {
            cqlBuilder.append(keyspace).append(".").append(tableName);
        }
        
        // Insert columns
        cqlBuilder.append(" (");
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) {
                cqlBuilder.append(", ");
            }
            if (quoteIdentifiers) {
                cqlBuilder.append(quoteIdentifier(columnNames.get(i)));
            } else {
                cqlBuilder.append(columnNames.get(i));
            }
        }
        
        // Values placeholders
        cqlBuilder.append(") VALUES (");
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) {
                cqlBuilder.append(", ");
            }
            cqlBuilder.append("?");
        }
        cqlBuilder.append(")");
        
        // Add IF NOT EXISTS if required
        if (isInsertIfNotExists) {
            cqlBuilder.append(" IF NOT EXISTS");
        }
        
        // Add TTL if configured
        if (ttlSeconds > 0) {
            cqlBuilder.append(" USING TTL ").append(ttlSeconds);
        }
        
        String cql = cqlBuilder.toString();
        log.debug("Prepared statement CQL: {}", cql);
        
        return session.prepare(cql);
    }

    private String getCacheKey(boolean isInsertIfNotExists, String tableName) {
    return String.format("%s.%s-%s-%d", 
            config.getKeyspace(), 
            tableName, 
            isInsertIfNotExists ? "if_not_exists" : "normal", 
            ttlSeconds);
}

    private String[] parseContactPoints(String connectionUrl) {
        // Simple parsing of connection URL
        // Format: cassandra://host1,host2:9042
        String url = connectionUrl.trim();
        if (url.startsWith("cassandra://")) {
            url = url.substring("cassandra://".length());
        }
        
        // Strip port if present
        int portIndex = url.lastIndexOf(':');
        if (portIndex > 0) {
            url = url.substring(0, portIndex);
        }
        
        return url.split(",");
    }

    private int parsePort(String connectionUrl) {
        // Default Cassandra port
        int port = 9042;
        
        int portIndex = connectionUrl.lastIndexOf(':');
        if (portIndex > 0) {
            try {
                port = Integer.parseInt(connectionUrl.substring(portIndex + 1));
            } catch (NumberFormatException e) {
                // Use default port
            }
        }
        
        return port;
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private String normalizeIdentifier(String identifier) {
        return config.getNormalizeCase() ? identifier.toLowerCase() : identifier;
    }

    @Override
    public void close() {
        if (session != null) {
            try {
                session.close();
            } catch (Exception e) {
                log.warn("Error closing session", e);
            }
        }
        
        if (cluster != null) {
            try {
                cluster.close();
            } catch (Exception e) {
                log.warn("Error closing cluster", e);
            }
        }
    }
}