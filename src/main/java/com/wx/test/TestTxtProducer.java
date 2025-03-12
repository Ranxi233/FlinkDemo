package com.wx.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class TestTxtProducer {


    public static void main(String[] args) throws IOException {
        String topicName = "order_topic";
        String filePath = "D:\\my_project\\idea_workplace\\kafka\\src\\main\\resources\\order.txt";

        Properties props = new Properties();
        props.put("bootstrap.servers", "xnn1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                producer.send(new ProducerRecord<>(topicName, line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}