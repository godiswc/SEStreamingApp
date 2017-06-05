package com.cloudera.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Created by root on 5/30/17.
 */
public class SEKafkaProducer {

    private KafkaProducer<String,String> producer;

    public SEKafkaProducer(){
        Properties props = new Properties();
        //kafka broker list
        props.put("bootstrap.servers", "192.168.10.5:9092,192.168.10.6:9092,192.168.10.7:9092,192.168.10.8:9092,192.168.10.9:9092,192.168.10.10:9092,192.168.10.12:9092");
        props.put("acks","all");
        props.put("batch.size",16384);
        props.put("linger.ms",1);
        props.put("buffer.memory",33554432);
        //serializer
        props.put("serializer.class", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer.class", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
    }

    public void produce(){
        while(true){

        }
    }


    public void close(){
        producer.close();
    }

}
