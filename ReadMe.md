# Kafka Example
In this project will be shown how to use kafka in Java without using any framework. According to initial suppose, our
kafka server located on server with 10.12.47.125 IP address, if you wanna use this project you ought to replace `10.12.47.125:9092` 
with `<your_kafka_server>:<kafka_port>` entire of project. In this project `Producer.class` and `Consumer.class` have been considered to manage
producer ans consumer functions in using of kafka provider. `MainProducer.class` and `MainConsumer.class` have been considered
to use of both classes `Producer.class` and `Consumer.class`. by running `MainProducer.class` you able to put information of two
users `Alireza` and `Morteza` into topic that named `user_registered` and by running `MainConsumer.class` you can withdraw 
user's information form topic `user_registered`. 

![Kafka-Example](pics/kafka-example.png)

