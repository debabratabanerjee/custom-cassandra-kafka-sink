package io.kpininja.connect.cassandra;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Configuration for the Cassandra Sink Connector
 */
public class CassandraSinkConfig extends AbstractConfig {

    // Connection settings
    public static final String CONNECTION_URL_CONFIG = "connection.url";
    private static final String CONNECTION_URL_DOC = "Cassandra connection URL in format: cassandra://host1,host2:9042";
    private static final String CONNECTION_URL_DISPLAY = "Connection URL";

    public static final String CONNECTION_USER_CONFIG = "connection.user";
    private static final String CONNECTION_USER_DOC = "Cassandra connection username for authentication.";
    private static final String CONNECTION_USER_DISPLAY = "Connection Username";

    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    private static final String CONNECTION_PASSWORD_DOC = "Cassandra connection password for authentication.";
    private static final String CONNECTION_PASSWORD_DISPLAY = "Connection Password";

    // Connection Pool settings
    public static final String MAX_CONNECTIONS_PER_HOST_CONFIG = "max.connections.per.host";
    private static final String MAX_CONNECTIONS_PER_HOST_DOC = "Maximum number of connections to maintain per host.";
    private static final String MAX_CONNECTIONS_PER_HOST_DISPLAY = "Max Connections Per Host";
    public static final int MAX_CONNECTIONS_PER_HOST_DEFAULT = 8;
    
    public static final String CONNECTION_TIMEOUT_MS_CONFIG = "connection.timeout.ms";
    private static final String CONNECTION_TIMEOUT_MS_DOC = "Connection timeout in milliseconds.";
    private static final String CONNECTION_TIMEOUT_MS_DISPLAY = "Connection Timeout (ms)";
    public static final int CONNECTION_TIMEOUT_MS_DEFAULT = 5000;

    // Target settings
    public static final String KEYSPACE_CONFIG = "keyspace";
    private static final String KEYSPACE_DOC = "The Cassandra keyspace to write to.";
    private static final String KEYSPACE_DISPLAY = "Keyspace";

    public static final String TABLE_CONFIG = "table";
    private static final String TABLE_DOC = "The default Cassandra table to write to (if not specified by topic mapping).";
    private static final String TABLE_DISPLAY = "Default Table";

    public static final String TOPIC_TO_TABLE_MAP_CONFIG = "topic.table.map";
    private static final String TOPIC_TO_TABLE_MAP_DOC = "Comma-separated list of mappings in the form 'topic:table'. If not specified, topic name will be used as table name.";
    private static final String TOPIC_TO_TABLE_MAP_DISPLAY = "Topic to Table Mapping";
    public static final String TOPIC_TO_TABLE_MAP_DEFAULT = "";

    // Primary key validation
    public static final String PK_FIELDS_CONFIG = "pk.fields";
    private static final String PK_FIELDS_DOC = "Comma-separated list of field names that are part of the primary key. Required to validate record schemas.";
    private static final String PK_FIELDS_DISPLAY = "Primary Key Fields";

    // Write behavior
    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "Specifies how many records to attempt to batch together for insertion into the destination table, when possible.";
    private static final String BATCH_SIZE_DISPLAY = "Batch Size";
    public static final int BATCH_SIZE_DEFAULT = 100;

    public static final String INSERT_MODE_CONFIG = "insert.mode";
    private static final String INSERT_MODE_DOC = "The insertion mode to use. 'INSERT' (standard insertion, may overwrite existing rows), 'INSERT_IF_NOT_EXISTS' (insertion only if row doesn't exist).";
    private static final String INSERT_MODE_DISPLAY = "Insert Mode";
    
    public static final String INSERT_MODE_INSERT = "INSERT";
    public static final String INSERT_MODE_INSERT_IF_NOT_EXISTS = "INSERT_IF_NOT_EXISTS";
    public static final String INSERT_MODE_DEFAULT = INSERT_MODE_INSERT;

    public static final String CONSISTENCY_LEVEL_CONFIG = "consistency.level";
    private static final String CONSISTENCY_LEVEL_DOC = "The consistency level to use for write operations (ONE, TWO, THREE, QUORUM, ALL, LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM).";
    private static final String CONSISTENCY_LEVEL_DISPLAY = "Consistency Level";
    public static final String CONSISTENCY_LEVEL_DEFAULT = "LOCAL_QUORUM";
    private static final String[] VALID_CONSISTENCY_LEVELS = {
        "ONE", "TWO", "THREE", "QUORUM", "ALL", 
        "LOCAL_ONE", "LOCAL_QUORUM", "EACH_QUORUM"
    };

    public static final String TTL_CONFIG = "ttl.seconds";
    private static final String TTL_DOC = "Time-to-live in seconds for written records. Set to 0 for no TTL.";
    private static final String TTL_DISPLAY = "TTL (seconds)";
    public static final int TTL_DEFAULT = 0;

    // SQL formatting options
    public static final String QUOTE_IDENTIFIERS_CONFIG = "quote.identifiers";
    private static final String QUOTE_IDENTIFIERS_DOC = "Whether to quote all identifiers in CQL statements.";
    private static final String QUOTE_IDENTIFIERS_DISPLAY = "Quote Identifiers";
    public static final boolean QUOTE_IDENTIFIERS_DEFAULT = false;

    public static final String NORMALIZE_CASE_CONFIG = "normalize.case";
    private static final String NORMALIZE_CASE_DOC = "Convert all identifiers to lowercase (recommended for case-insensitive Cassandra namespaces)";
    private static final String NORMALIZE_CASE_DISPLAY = "Normalize Case";
    public static final boolean NORMALIZE_CASE_DEFAULT = true;

    // Error handling
    public static final String MAX_RETRIES_CONFIG = "max.retries";
    private static final String MAX_RETRIES_DOC = "Maximum number of times to retry on retriable errors before failing the task.";
    private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";
    public static final int MAX_RETRIES_DEFAULT = 10;

    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    private static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following a retriable error before a retry attempt.";
    private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (ms)";
    public static final int RETRY_BACKOFF_MS_DEFAULT = 3000;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            // Connection
            .define(
                    CONNECTION_URL_CONFIG,
                    Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH,
                    CONNECTION_URL_DOC,
                    "Connection",
                    0,
                    Width.LONG,
                    CONNECTION_URL_DISPLAY
            )
            .define(
                    CONNECTION_USER_CONFIG,
                    Type.STRING,
                    null,
                    Importance.MEDIUM,
                    CONNECTION_USER_DOC,
                    "Connection",
                    1,
                    Width.MEDIUM,
                    CONNECTION_USER_DISPLAY
            )
            .define(
                    CONNECTION_PASSWORD_CONFIG,
                    Type.PASSWORD,
                    null,
                    Importance.MEDIUM,
                    CONNECTION_PASSWORD_DOC,
                    "Connection",
                    2,
                    Width.MEDIUM,
                    CONNECTION_PASSWORD_DISPLAY
            )
            .define(
                    MAX_CONNECTIONS_PER_HOST_CONFIG,
                    Type.INT,
                    MAX_CONNECTIONS_PER_HOST_DEFAULT,
                    ConfigDef.Range.atLeast(1),
                    Importance.MEDIUM,
                    MAX_CONNECTIONS_PER_HOST_DOC,
                    "Connection",
                    3,
                    Width.SHORT,
                    MAX_CONNECTIONS_PER_HOST_DISPLAY
            )
            .define(
                    CONNECTION_TIMEOUT_MS_CONFIG,
                    Type.INT,
                    CONNECTION_TIMEOUT_MS_DEFAULT,
                    ConfigDef.Range.atLeast(500),
                    Importance.MEDIUM,
                    CONNECTION_TIMEOUT_MS_DOC,
                    "Connection",
                    4,
                    Width.SHORT,
                    CONNECTION_TIMEOUT_MS_DISPLAY
            )
            // Target
            .define(
                    KEYSPACE_CONFIG,
                    Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH,
                    KEYSPACE_DOC,
                    "Target",
                    0,
                    Width.MEDIUM,
                    KEYSPACE_DISPLAY
            )
            .define(
                    TABLE_CONFIG,
                    Type.STRING,
                    "",
                    Importance.MEDIUM,
                    TABLE_DOC,
                    "Target",
                    1,
                    Width.MEDIUM,
                    TABLE_DISPLAY
            )
            .define(
                    TOPIC_TO_TABLE_MAP_CONFIG,
                    Type.STRING,
                    TOPIC_TO_TABLE_MAP_DEFAULT,
                    Importance.MEDIUM,
                    TOPIC_TO_TABLE_MAP_DOC,
                    "Target",
                    2,
                    Width.MEDIUM,
                    TOPIC_TO_TABLE_MAP_DISPLAY
            )
            .define(
                    PK_FIELDS_CONFIG,
                    Type.STRING,
                    "",
                    Importance.HIGH,
                    PK_FIELDS_DOC,
                    "Target",
                    3,
                    Width.MEDIUM,
                    PK_FIELDS_DISPLAY
            )
            // Write behavior
            .define(
                    BATCH_SIZE_CONFIG,
                    Type.INT,
                    BATCH_SIZE_DEFAULT,
                    ConfigDef.Range.atLeast(1),
                    Importance.MEDIUM,
                    BATCH_SIZE_DOC,
                    "Writes",
                    0,
                    Width.SHORT,
                    BATCH_SIZE_DISPLAY
            )
            .define(
                    INSERT_MODE_CONFIG,
                    Type.STRING,
                    INSERT_MODE_DEFAULT,
                    ValidString.in(INSERT_MODE_INSERT, INSERT_MODE_INSERT_IF_NOT_EXISTS),
                    Importance.HIGH,
                    INSERT_MODE_DOC,
                    "Writes",
                    1,
                    Width.MEDIUM,
                    INSERT_MODE_DISPLAY
            )
            .define(
                    CONSISTENCY_LEVEL_CONFIG,
                    Type.STRING,
                    CONSISTENCY_LEVEL_DEFAULT,
                    ValidString.in(VALID_CONSISTENCY_LEVELS),
                    Importance.MEDIUM,
                    CONSISTENCY_LEVEL_DOC,
                    "Writes",
                    2,
                    Width.MEDIUM,
                    CONSISTENCY_LEVEL_DISPLAY
            )
            .define(
                    TTL_CONFIG,
                    Type.INT,
                    TTL_DEFAULT,
                    ConfigDef.Range.atLeast(0),
                    Importance.LOW,
                    TTL_DOC,
                    "Writes",
                    3,
                    Width.SHORT,
                    TTL_DISPLAY
            )
            // SQL formatting
            .define(
                QUOTE_IDENTIFIERS_CONFIG,
                Type.BOOLEAN,
                QUOTE_IDENTIFIERS_DEFAULT,
                Importance.MEDIUM,
                QUOTE_IDENTIFIERS_DOC,
                "Formatting",
                0,
                Width.SHORT,
                QUOTE_IDENTIFIERS_DISPLAY
            )
            .define(
                NORMALIZE_CASE_CONFIG,
                Type.BOOLEAN,
                NORMALIZE_CASE_DEFAULT,
                Importance.HIGH,
                NORMALIZE_CASE_DOC,
                "Formatting",
                1,
                Width.SHORT,
                NORMALIZE_CASE_DISPLAY
            )
            // Error handling
            .define(
                MAX_RETRIES_CONFIG,
                Type.INT,
                MAX_RETRIES_DEFAULT,
                ConfigDef.Range.atLeast(0),
                Importance.MEDIUM,
                MAX_RETRIES_DOC,
                "Error Handling",
                0,
                Width.SHORT,
                MAX_RETRIES_DISPLAY
            )
            .define(
                RETRY_BACKOFF_MS_CONFIG,
                Type.INT,
                RETRY_BACKOFF_MS_DEFAULT,
                ConfigDef.Range.atLeast(100),
                Importance.MEDIUM,
                RETRY_BACKOFF_MS_DOC,
                "Error Handling",
                1,
                Width.SHORT,
                RETRY_BACKOFF_MS_DISPLAY
            );

    public CassandraSinkConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
        validateConfig();
    }

    private void validateConfig() {
        // Validate connection URL
        String connectionUrl = getConnectionUrl();
        if (!connectionUrl.contains(":")) {
            throw new ConfigException(CONNECTION_URL_CONFIG, 
                connectionUrl, "Connection URL must include at least one host and optionally a port");
        }
        
        // If explicit table mapping is not provided, ensure table is set
        Map<String, String> topicToTableMap = getTopicToTableMap();
        if (topicToTableMap.isEmpty() && getTable().isEmpty()) {
            // This is actually okay - we'll use the topic name as the table name
        }
        
        // Validate TTL
        int ttl = getTtl();
        if (ttl < 0) {
            throw new ConfigException(TTL_CONFIG, ttl, "TTL must be non-negative");
        }
        
        // Validate batch size - too large can cause performance issues
        int batchSize = getBatchSize();
        if (batchSize > 1000) {
            throw new ConfigException(BATCH_SIZE_CONFIG, batchSize, 
                "Batch size above 1000 may cause performance issues in Cassandra");
        }

        // Make PK fields optional when in INSERT mode
        String insertMode = getInsertMode();
        if (INSERT_MODE_INSERT_IF_NOT_EXISTS.equals(insertMode)) {
            // For INSERT_IF_NOT_EXISTS, PK fields are still required
            List<String> pkFields = getPkFields();
            if (pkFields.isEmpty()) {
                throw new ConfigException(PK_FIELDS_CONFIG, 
                    "Primary key fields must be specified when using INSERT_IF_NOT_EXISTS mode");
            }
        }
    }

    public String getConnectionUrl() {
        return getString(CONNECTION_URL_CONFIG);
    }

    public String getConnectionUser() {
        return getString(CONNECTION_USER_CONFIG);
    }

    public String getConnectionPassword() {
        return getPassword(CONNECTION_PASSWORD_CONFIG).value();
    }

    public int getMaxConnectionsPerHost() {
        return getInt(MAX_CONNECTIONS_PER_HOST_CONFIG);
    }
    
    public int getConnectionTimeoutMs() {
        return getInt(CONNECTION_TIMEOUT_MS_CONFIG);
    }

    public String getKeyspace() {
        return getString(KEYSPACE_CONFIG);
    }

    public String getTable() {
        return getString(TABLE_CONFIG);
    }

    public int getBatchSize() {
        return getInt(BATCH_SIZE_CONFIG);
    }

    public String getInsertMode() {
        return getString(INSERT_MODE_CONFIG);
    }

    public String getConsistencyLevel() {
        return getString(CONSISTENCY_LEVEL_CONFIG);
    }

    public int getTtl() {
        return getInt(TTL_CONFIG);
    }

    public boolean getQuoteIdentifiers() {
        return getBoolean(QUOTE_IDENTIFIERS_CONFIG);
    }

    public boolean getNormalizeCase() {
        return getBoolean(NORMALIZE_CASE_CONFIG);
    }
    
    public int getMaxRetries() {
        return getInt(MAX_RETRIES_CONFIG);
    }
    
    public int getRetryBackoffMs() {
        return getInt(RETRY_BACKOFF_MS_CONFIG);
    }

    public Map<String, String> getTopicToTableMap() {
        String mappings = getString(TOPIC_TO_TABLE_MAP_CONFIG);
        Map<String, String> map = new HashMap<>();
        
        if (mappings != null && !mappings.isEmpty()) {
            for (String mapping : mappings.split(",")) {
                String[] parts = mapping.trim().split(":");
                if (parts.length == 2) {
                    map.put(parts[0].trim(), parts[1].trim());
                }
            }
        }
        
        return map;
    }
    
    public List<String> getPkFields() {
        String pkFields = getString(PK_FIELDS_CONFIG);
        if (pkFields == null || pkFields.isEmpty()) {
            return Collections.emptyList();
        }
        
        return Arrays.stream(pkFields.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }
}