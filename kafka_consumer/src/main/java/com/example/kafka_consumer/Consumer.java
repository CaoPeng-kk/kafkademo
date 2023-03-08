package com.example.kafka_consumer;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @Description:
 * @Date: Created in 17:56 2023/2/25 0025
 * @Author: caopeng
 */
public class Consumer {

    public static void main(String[] args) throws InterruptedException {
        Map<String, String> kafkaProperties = new HashMap<>();

        // 设置集群地址
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.20.122:9092");
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class.getName());
        // 设置group id
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "cp");
        // 关闭自动提交
        kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(kafkaProperties);

        // 订阅topic
        List<String> topic = new ArrayList<>();
        topic.add("caopeng");
        kafkaConsumer.subscribe(topic);

        while (true) {
            System.out.println(" --- 开始拉取数据 --- ");
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            TimeUnit.SECONDS.sleep(5);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }
}
