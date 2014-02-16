package com.gg;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * Created by gaoge on 2/16/14.
 */
public class CuratorConn {

    private final static String zookeeperConnectionString = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    public static void main(String args[])throws Exception{

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy);
        client.start();
        client.create().withMode(CreateMode.EPHEMERAL ).forPath("/job/feature", "feature_job_1".getBytes());
        client.close();
    }
}
