package com.cloudera.launcher;

import com.cloudera.common.SEStreamingConstants;
import com.cloudera.kafka.KafkaTopicBean;
import com.cloudera.kafka.KafkaUtil;
import com.cloudera.kafka.SEKafkaProducer;
import com.onecloud.tsdb.demo.TSDBDataRetriever;
import onecloud.plantpower.database.driver.protobuf.Driver;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
/**
 * Created by root on 5/31/17.
 */
public class DataProducer {



    public DataProducer(){


    }

    public KafkaTopicBean genKafkaTopicBean(Properties prop){
        KafkaTopicBean bean;
        String topic = prop.getProperty(SEStreamingConstants.TOPIC);
        int partition = Integer.parseInt(prop.getProperty(SEStreamingConstants.TOPIC_PARTITION));
        int replicationNum = Integer.parseInt(prop.getProperty(SEStreamingConstants.REPLICATION_NUM));
        String describe = prop.getProperty(SEStreamingConstants.DESCRIBE);
        int operationType = Integer.parseInt(prop.getProperty(SEStreamingConstants.OPERATION_TYPE));
        bean = new KafkaTopicBean(topic,partition,replicationNum,describe,operationType);
        return bean;
    }


    public static void main(String[] args){
        String propFile = "./conf/producer.properties";
        Properties prop = new Properties();
        DataProducer dp = new DataProducer();
        try {
            prop.load(new FileInputStream(propFile));
            String host = prop.getProperty(SEStreamingConstants.TSDB_HOST);
            int port = Integer.parseInt(prop.getProperty(SEStreamingConstants.TSDB_PORT));
            TSDBDataRetriever retriever = new TSDBDataRetriever(host,port);
            SEKafkaProducer producer = new SEKafkaProducer(prop);
            String zkstr = prop.getProperty(SEStreamingConstants.ZKSTR);
            KafkaUtil.createTopic(zkstr,dp.genKafkaTopicBean(prop));

            while(true){
                Driver.FindDataPointResponse response = retriever.retrieve(prop);

                producer.send(response);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
