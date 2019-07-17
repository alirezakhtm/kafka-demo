package com.khtm.test.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    private final KafkaProducer<String, String> mProducer;
    private final Logger mLogger = LoggerFactory.getLogger(Producer.class);

    private Properties producerProperties(String bootstrapServer){
        String serializer = StringSerializer.class.getName();
        Properties prob = new Properties();
        prob.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        prob.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        prob.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        return prob;
    }

    public Producer(String bootstrapServer){
        Properties props = producerProperties(bootstrapServer);
        mProducer = new KafkaProducer<String, String>(props);
        mLogger.info("Producer Initialized.");
    }

    public void put(String topic, String key, String value) throws ExecutionException, InterruptedException {
        mLogger.info("Put value: " + value + ", for key: " + key);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        mProducer.send(record, (recordMetadata, e) -> {
            if (e != null){
                mLogger.error("Error while producing", e);
                return;
            }

            mLogger.info("Received new meta.\n" +
                    "Topic: " + recordMetadata.topic() + "\n" +
                    "Partition: " + recordMetadata.partition() + "\n" +
                    "Offset: " + recordMetadata.offset() + "\n" +
                    "Timestamp: " + recordMetadata.timestamp());
        }).get();
    }

    public void close(){
        mLogger.info("Closing producer connection.");
        mProducer.close();
    }

}
