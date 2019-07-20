package com.khtm.test.kafka.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.VM;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {

    private final Logger mLogger = LoggerFactory.getLogger(Consumer.class.getName());
    private final String mBootstrapServer;
    private final String mGroupId;
    private final String mTopic;

    public Consumer(String bootstrapServer, String groupId, String topic){
        this.mBootstrapServer = bootstrapServer;
        this.mGroupId = groupId;
        this.mTopic = topic;
    }

    public void run(){
        mLogger.info("Creating consumer thread.");

        CountDownLatch countDownLatch = new CountDownLatch(1);

        ConsumerRunnable consumerRunnable =
                new ConsumerRunnable(this.mBootstrapServer, this.mGroupId, this.mTopic, countDownLatch);
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            mLogger.info("Caught shutdown hook.");
            consumerRunnable.shutdown();
            await(countDownLatch);
        }));

        await(countDownLatch);
    }

    void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            mLogger.error("Application got interrupted", e);
        } finally {
            mLogger.info("Application is closing");
        }
    }

    private class ConsumerRunnable implements Runnable {

        private CountDownLatch mLatch;
        private KafkaConsumer<String, String> mConsumer;

        public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch){
            this.mLatch = latch;
            Properties properties = consumerProps(bootstrapServer, groupId);
            this.mConsumer = new KafkaConsumer<String, String>(properties);
            this.mConsumer.subscribe(Collections.singletonList(topic));
        }

        private Properties consumerProps(String bootStrapServer, String groupId){
            String deserializer = StringDeserializer.class.getName();
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return properties;
        }

        @Override
        public void run() {
            try {
                do{
                    ConsumerRecords<String, String> records = this.mConsumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String, String> record : records){
                        mLogger.info("Key: " + record.key() + ", Value: " + record.value());
                        mLogger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }while (true);
            } catch (WakeupException e){
                mLogger.info("Received shutdown signal!");
            }finally {
                mConsumer.close();
                mLatch.countDown();
            }
        }

        public void shutdown(){
            this.mConsumer.wakeup();
        }
    }
}
