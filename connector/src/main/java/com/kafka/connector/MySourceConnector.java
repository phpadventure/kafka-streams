package com.kafka.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MySourceConnector extends SourceConnector {

    private Map<String, String> configProps;


    @Override
    public void start(Map<String, String> props) {
        configProps = props; // this can contain DB url, etc. Tasks.masks. Topics to read from etc.
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MySourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // Provides configuration for each task instance
        List<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(Map.copyOf(configProps)); // Each task gets the same config in this example
        }
        return configs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return MyConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1.0";
    }
}
