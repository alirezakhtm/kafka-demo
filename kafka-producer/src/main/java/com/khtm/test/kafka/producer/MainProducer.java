package com.khtm.test.kafka.producer;

import java.util.concurrent.ExecutionException;

public class MainProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String server = "10.12.47.125:9092";
        String topic = "user_registered";

        Producer producer = new Producer(server);
        producer.put(topic, "user1", "Alireza");
        producer.put(topic, "user2", "Morteza");
        producer.close();
    }

}
