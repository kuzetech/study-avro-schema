package com.kuze.bigdata.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kuze.bigdata.domain.OdpsTableRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class JsonProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        Random rd = new Random();
        ObjectMapper objectMapper = new ObjectMapper();

        for (int i = 0; i < 1000; i++) {
            OdpsTableRecord record = new OdpsTableRecord(String.valueOf(i),String.valueOf(i),"prod", "2012-01-"+rd.nextInt(1));
            String jsonRecord = objectMapper.writeValueAsString(record);
            ProducerRecord<String, String> record1 = new ProducerRecord<>("odps1", jsonRecord);
            //ProducerRecord<String, String> record2 = new ProducerRecord<>("odps2", jsonRecord);
            producer.send(record1);
            //producer.send(record2);
        }

        producer.close();
    }
}
