package io.kpininja.connect.cassandra;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Production-Ready Kafka Connect Cassandra sink connector.
 */
public class CassandraSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(CassandraSinkConnector.class);

    private Map<String, String> configProps;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Cassandra Sink Connector version {}", version());
        
        // Validate configuration
        try {
            new CassandraSinkConfig(props);
        } catch (Exception e) {
            throw new ConnectException("Failed to start connector due to configuration error: " + e.getMessage(), e);
        }
        
        configProps = new HashMap<>(props);
        log.info("Cassandra Sink Connector started");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CassandraSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating task configurations, maxTasks={}", maxTasks);
        
        // Create task configs
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        
        // For each task, create a config with the task.id property
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskConfig = new HashMap<>(configProps);
            taskConfig.put("task.id", String.valueOf(i));
            configs.add(taskConfig);
        }
        
        log.debug("Created {} task configurations", configs.size());
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopping Cassandra Sink Connector");
        // No resources to clean up
    }

    @Override
    public ConfigDef config() {
        return CassandraSinkConfig.CONFIG_DEF;
    }
}