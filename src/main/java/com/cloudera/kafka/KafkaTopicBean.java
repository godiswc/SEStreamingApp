package com.cloudera.kafka;

/**
 * Created by root on 5/30/17.
 */
public class KafkaTopicBean {
    private String topic;
    private int partiton;
    private int replicaNum;
    private String descrbe;
    private int operationType;


    public KafkaTopicBean() {
    }

    public KafkaTopicBean(String topic, int partiton, int replicaNum, String descrbe, int operationType) {
        this.topic = topic;
        this.partiton = partiton;
        this.replicaNum = replicaNum;
        this.descrbe = descrbe;
        this.operationType = operationType;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartiton() {
        return partiton;
    }

    public void setPartiton(int partiton) {
        this.partiton = partiton;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public void setReplicaNum(int replicaNum) {
        this.replicaNum = replicaNum;
    }

    public String getDescrbe() {
        return descrbe;
    }

    public void setDescrbe(String descrbe) {
        this.descrbe = descrbe;
    }

    public int getOperationType() {
        return operationType;
    }

    public void setOperationType(int operationType) {
        this.operationType = operationType;
    }

    @Override
    public String toString() {
        return "kafka Topic: "+topic+" partition num "+partiton+"replication Number: "+replicaNum;
    }
}
