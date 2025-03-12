package com.wx.test;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class TestTxtConsumer {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topicName = "maxwell";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "xnn1:9092");
        properties.setProperty("group.id", "test-consumer-group");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topicName,
                new SimpleStringSchema(),
                properties
        );

        env.addSource(kafkaConsumer)
                .print();

        env.execute("Kafka to Flink");
    }
}