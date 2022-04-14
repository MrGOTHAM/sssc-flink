package com.example.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: an
 * Date: 2022/4/12
 * Time: 10:19
 * Description:
 */
public class MyKafkaUtil {

    private static String brokers = "tao:9092";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {

        return new FlinkKafkaProducer<>(brokers, topic, new SimpleStringSchema());

    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }
}
