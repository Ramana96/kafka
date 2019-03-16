package com.modak;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class KafTopicCreation {

    public static Properties kafkaprop;

    public static void main(String[] args) {
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;

        kafkaprop = new Properties();

        String topicName = null;
        try {
            kafkaprop.load(new FileInputStream(new File(args[0])));

            String zookeeperHosts = kafkaprop.getProperty(
                "zookeeper"); // If multiple zookeeper then -> String zookeeperHosts = "192.168.20.1:2181,192.168.20.2:2181";

            int sessionTimeOutInMs = Integer.valueOf(kafkaprop.getProperty("sessionTimeOutInMs"));
            int connectionTimeOutInMs = Integer.valueOf(kafkaprop.getProperty("connectionTimeOutInMs"));

            zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs,
                ZKStringSerializer$.MODULE$);

//
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);
//
            topicName = kafkaprop.getProperty("topicName");
            int noOfPartitions = Integer.valueOf(kafkaprop.getProperty("noOfPartitions"));
            int noOfReplication = Integer.valueOf(kafkaprop.getProperty("noOfReplication"));
            Properties topicConfiguration;
            topicConfiguration = new Properties();
            AdminUtils.createTopic(zkUtils, topicName, noOfPartitions, noOfReplication, topicConfiguration,
                RackAwareMode.Enforced$.MODULE$);

            System.out.println(topicName + "  noOfPartitions " + noOfPartitions + " is created");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            zkClient.close();
        }

    }


}
