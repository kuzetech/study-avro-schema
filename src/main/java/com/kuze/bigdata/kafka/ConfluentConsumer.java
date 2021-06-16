package com.kuze.bigdata.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConfluentConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", KafkaConstants.GROUP_ID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 使用Confluent实现的KafkaAvroDeserializer
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        // 添加schema服务的地址，用于获取schema
        props.put("schema.registry.url", "http://localhost:8081");
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);

        List<String> list = new ArrayList<>();
        list.add(KafkaConstants.TOPIC_USER);
        list.add(KafkaConstants.TOPIC_WALLET);
        consumer.subscribe(list);

        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(1000);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    GenericRecord user = record.value();
                    System.out.println(user.toString());
                    /*System.out.println("value = [user.id = " + user.get("id") + ", " + "user.name = "
                            + user.get("name") + ", " + "user.age = " + user.get("age") + "], "
                            + "partition = " + record.partition() + ", " + "offset = " + record.offset());*/

                }
            }
        } finally {
            consumer.close();
        }
    }
}
