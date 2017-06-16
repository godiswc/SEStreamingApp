package com.cloudera.common;

/**
 * Created by root on 5/31/17.
 */
public class SEStreamingConstants {


    public final static String POINTS_LIST = "points_list";
    public final static String SEP = "\\;";
    public final static String DELIMITER = "\\,";
    public final static String TSDB_HOST="TSDB_HOST";
    public final static String TSDB_PORT="TSDB_PORT";


    //conf for kafka producer
    public final static String SERVERS= "bootstrap.servers";
    public final static String ACKS="acks";
    public final static String BATCHSIZE="batch.size";
    public final static String LINGERMS="linger.ms";
    public final static String BUFFERMEMORY="buffer.memory";

    public final static String TOPIC="topic";
    public final static String TOPIC_PARTITION="partition";
    public final static String REPLICATION_NUM="replicationNum";
    public final static String DESCRIBE="describe";
    public final static String OPERATION_TYPE="operationType";
    public final static String FETCH_TIME_OFFSET="fetch.offset";
    public final static String FETCH_MAX_INTERVAL="fetch.max.interval";
    public final static String TSDBINTERVAL="tsdb.interval";

    public final static String ZKSTR="zkstr";


    public final static String CONSUMERTOPICS="consumer.topics";
    public final static String CONSUMERNUMTHREADS="consumer.numThreads";
    public final static String HBASETABLE="hbaseTable";
    public final static String HBASESPLITKEYS="hbaseSplitKey";
    public final static String HBASECF="hbaseCF";
    public final static String CONSUMEROFFSET="consumer.offset";
    public final static String ZOOKEEPERCONNECT="zookeeper.connect";
    public final static String WINDOWSECONDS="window.seconds";


    //Machine learning
    public final static String LABELNAME="labelName";
    public final static String STEPSIZE="stepSize";

}
