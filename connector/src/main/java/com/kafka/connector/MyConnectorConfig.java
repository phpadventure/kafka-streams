package com.kafka.connector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class MyConnectorConfig extends AbstractConfig {
    public static final String TOPIC_WRITE_TO = "topic.write.to";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(TOPIC_WRITE_TO, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Topic to write to");

    public MyConnectorConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }
}
