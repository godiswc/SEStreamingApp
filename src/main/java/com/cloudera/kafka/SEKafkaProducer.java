package com.cloudera.kafka;

import com.cloudera.common.SEStreamingConstants;
import onecloud.plantpower.database.driver.protobuf.Driver;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by root on 5/30/17.
 */
public class SEKafkaProducer {

    private KafkaProducer<String,byte[]> producer;
    private String topic;

    public SEKafkaProducer(Properties p){
        Properties props = new Properties();
        //kafka broker list
        props.put("bootstrap.servers",p.getProperty(SEStreamingConstants.SERVERS));
        props.put("acks",p.getProperty(SEStreamingConstants.ACKS));
        props.put("batch.size",p.getProperty(SEStreamingConstants.BATCHSIZE));
        props.put("linger.ms",p.getProperty(SEStreamingConstants.LINGERMS));
        props.put("buffer.memory",p.getProperty(SEStreamingConstants.BUFFERMEMORY));

        //props.put("bootstrap.servers", "192.168.10.5:9092,192.168.10.6:9092,192.168.10.7:9092,192.168.10.8:9092,192.168.10.9:9092,192.168.10.10:9092,192.168.10.12:9092");
        //props.put("acks","all");
        //props.put("batch.size",16384);
        //props.put("linger.ms",1);
        //props.put("buffer.memory",33554432);
        //serializer
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, byte[]>(props);

        topic = p.getProperty(SEStreamingConstants.TOPIC);
    }

    public void send(Driver.FindDataPointResponse response){
        producer.send(new ProducerRecord<String,byte[]>(topic,response.toByteArray()));

    }


    public void close(){
        producer.close();
    }

}
