package io.kpininja.connect.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Utility for version information
 */
public class VersionUtil {
    private static final Logger log = LoggerFactory.getLogger(VersionUtil.class);
    private static String version = "unknown";
    
    static {
        try {
            Properties props = new Properties();
            try (InputStream resourceStream = 
                     VersionUtil.class.getResourceAsStream("/cassandra-sink-connector-version.properties")) {
                if (resourceStream != null) {
                    props.load(resourceStream);
                    version = props.getProperty("version", version).trim();
                }
            }
        } catch (Exception e) {
            log.warn("Error while loading version:", e);
        }
    }
    
    public static String getVersion() {
        return version;
    }
}