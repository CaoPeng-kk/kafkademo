package com.example.kafka_producer.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description:
 * @Date: Created in 11:30 2023/1/4 0004
 * @Author: caopeng
 */
public class Producer {

    public static void main(String[] args) {

        Map<String, String> kafkaProperties = new HashMap<>();

        // 设置集群地址
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.20.122:9092");
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer kafkaProducer = new KafkaProducer(kafkaProperties);

        int i = 0;
        while (i < 20) {
            ProducerRecord<String, String> record = new ProducerRecord<>("caopeng", "caopeng - " + i);

            kafkaProducer.send(record);
            i++;
        }

        kafkaProducer.close();
    }
}
