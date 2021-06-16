package com.kuze.bigdata.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.Properties;

public class ConfluentProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 使用Confluent实现的KafkaAvroSerializer
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        // 添加schema服务的地址，用于获取schema
        props.put("schema.registry.url", "http://localhost:8081");

        Producer<String, GenericRecord> producer = new KafkaProducer<>(props);
        Schema.Parser parser = new Schema.Parser();

        File userSchemaFile = new File("schema/user.json");
        Schema userSchema = parser.parse(userSchemaFile);
        for (int i = 0; i < 100; i++) {
            GenericRecord user1 = new GenericData.Record(userSchema);
            user1.put("userId", 1);
            user1.put("name", "kuze1");
            user1.put("age", 1);
            GenericRecord user2 = new GenericData.Record(userSchema);
            user2.put("userId", 2);
            user2.put("name", "kuze2");
            user2.put("age", 2);
            ProducerRecord<String, GenericRecord> record1 = new ProducerRecord<>("test1", user1);
            ProducerRecord<String, GenericRecord> record2 = new ProducerRecord<>("test2", user2);
            producer.send(record1);
            producer.send(record2);
            Thread.sleep(1000);
        }



        /*File walletSchemaFile = new File("schema/wallet.json");
        Schema walletSchema = parser.parse(walletSchemaFile);
        GenericRecord wallet = new GenericData.Record(walletSchema);
        wallet.put("userId", 1);
        wallet.put("money", 10);
        ProducerRecord<String, GenericRecord> record2 = new ProducerRecord<>(KafkaConstants.TOPIC_WALLET, wallet);
        producer.send(record2);*/

        producer.close();
    }
}
