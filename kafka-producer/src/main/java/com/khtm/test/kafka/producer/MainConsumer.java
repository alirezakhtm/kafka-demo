package com.khtm.test.kafka.producer;

public class MainConsumer {


    public static void main(String[] args) {
        String server = "10.12.47.125:9092";
        //String groupId = "some_application";
        //String topic = "user_registered";

        String groupId = "some_tes";
        String topic = "MCB.NAVACO.JPOS.M1100";

        new Consumer(server, groupId, topic).run();
    }

}
