package io.kpininja.connect.cassandra;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CassandraSinkConfig extends AbstractConfig {

    public static final String CONNECTION_URL_CONFIG = "connection.url";
    private static final String CONNECTION_URL_DOC = "Cassandra connection URL.";
    private static final String CONNECTION_URL_DISPLAY = "Connection URL";

    public static final String CONNECTION_USER_CONFIG = "connection.user";
    private static final String CONNECTION_USER_DOC = "Cassandra connection user.";
    private static final String CONNECTION_USER_DISPLAY = "Connection user";

    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    private static final String CONNECTION_PASSWORD_DOC = "Cassandra connection password.";
    private static final String CONNECTION_PASSWORD_DISPLAY = "Connection password";

    public static final String KEYSPACE_CONFIG = "keyspace";
    private static final String KEYSPACE_DOC = "The Cassandra keyspace to write to.";
    private static final String KEYSPACE_DISPLAY = "Keyspace";

    public static final String TABLE_CONFIG = "table";
    private static final String TABLE_DOC = "The Cassandra table to write to.";
    private static final String TABLE_DISPLAY = "Table";

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    private static final String BATCH_SIZE_DOC = "Specifies how many records to attempt to batch together for insertion into the destination table, when possible.";
    private static final String BATCH_SIZE_DISPLAY = "Batch Size";
    public static final int BATCH_SIZE_DEFAULT = 100;

    public static final String INSERT_MODE_CONFIG = "insert.mode";
    private static final String INSERT_MODE_DOC = "The insertion mode to use. Options: INSERT, INSERT_IF_NOT_EXISTS";
    private static final String INSERT_MODE_DISPLAY = "Insert Mode";
    
    public static final String INSERT_MODE_INSERT = "INSERT";
    public static final String INSERT_MODE_INSERT_IF_NOT_EXISTS = "INSERT_IF_NOT_EXISTS";
    public static final String INSERT_MODE_DEFAULT = INSERT_MODE_INSERT;

    public static final String CONSISTENCY_LEVEL_CONFIG = "consistency.level";
    private static final String CONSISTENCY_LEVEL_DOC = "The consistency level to use for write operations.";
    private static final String CONSISTENCY_LEVEL_DISPLAY = "Consistency Level";
    public static final String CONSISTENCY_LEVEL_DEFAULT = "LOCAL_QUORUM";

    public static final String TTL_CONFIG = "ttl.seconds";
    private static final String TTL_DOC = "Time-to-live in seconds for written records. Set to 0 for no TTL.";
    private static final String TTL_DISPLAY = "TTL (seconds)";
    public static final int TTL_DEFAULT = 0;

    public static final String QUOTE_IDENTIFIERS_CONFIG = "quote.identifiers";
    private static final String QUOTE_IDENTIFIERS_DOC = "Whether to quote all identifiers in CQL statements.";
    private static final String QUOTE_IDENTIFIERS_DISPLAY = "Quote Identifiers";
    public static final boolean QUOTE_IDENTIFIERS_DEFAULT = false;

    public static final String NORMALIZE_CASE_CONFIG = "normalize.case";
    private static final String NORMALIZE_CASE_DOC = "Convert all identifiers to lowercase";
    public static final boolean NORMALIZE_CASE_DEFAULT = true;
    private static final String NORMALIZE_CASE_DISPLAY = "Normlaize Case";

    public static final String TOPIC_TO_TABLE_MAP_CONFIG = "topic.table.map";
    private static final String TOPIC_TO_TABLE_MAP_DOC = "Comma-separated list of mappings in the form 'topic:table'";
    private static final String TOPIC_TO_TABLE_MAP_DISPLAY = "Topic Table Mapping";
    private static final String TOPIC_TO_TABLE_MAP_DEFAULT = "${topic}";

    public static final String PK_FIELDS_CONFIG = "pk.fields";
    private static final String PK_FIELDS_DOC = "Comma-separated list of fields that must be present as primary keys";


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
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
                    ConfigDef.NO_DEFAULT_VALUE,
                    Importance.MEDIUM,
                    TOPIC_TO_TABLE_MAP_DOC,
                    "Validation",
                    0,
                    Width.MEDIUM,
                    PK_FIELDS_DOC
            )
            .define(
                    BATCH_SIZE_CONFIG,
                    Type.INT,
                    BATCH_SIZE_DEFAULT,
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
                    ConfigDef.ValidString.in(INSERT_MODE_INSERT, INSERT_MODE_INSERT_IF_NOT_EXISTS),
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
                    Importance.LOW,
                    TTL_DOC,
                    "Writes",
                    3,
                    Width.SHORT,
                    TTL_DISPLAY
            )
            // Add to CONFIG_DEF (inside the existing CONFIG_DEF definition)
            .define(
                QUOTE_IDENTIFIERS_CONFIG,
                Type.BOOLEAN,
                QUOTE_IDENTIFIERS_DEFAULT,
                Importance.MEDIUM,
                QUOTE_IDENTIFIERS_DOC,
                "Writes",
                4,
                Width.SHORT,
                QUOTE_IDENTIFIERS_DISPLAY
            )
            .define(
                NORMALIZE_CASE_CONFIG,
                Type.BOOLEAN,
                NORMALIZE_CASE_DEFAULT,
                Importance.HIGH,
                NORMALIZE_CASE_DOC ,
                "Writes",
                5,
                Width.SHORT,
                NORMALIZE_CASE_DISPLAY
            );

    public CassandraSinkConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
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