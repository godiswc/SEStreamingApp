package com.cloudera.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;

import java.util.Properties;

/**
 * Created by root on 5/30/17.
 */
public class KafkaUtil {

    public KafkaUtil() {

    }

    public static void createTopic(String zkString, KafkaTopicBean bean) {
        ZkUtils zkUtils = ZkUtils.apply(zkString, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        AdminUtils.createTopic(zkUtils, bean.getTopic(), bean.getPartiton(), bean.getReplicaNum(), new Properties(), RackAwareMode.Enforced$.MODULE$);
        zkUtils.close();
    }

    public static void dropTopic(String zkString, KafkaTopicBean bean) {
        ZkUtils zkUtils = ZkUtils.apply(zkString, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        AdminUtils.deleteTopic(zkUtils, bean.getTopic());
        zkUtils.close();
    }
}