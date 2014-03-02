package com.gg.work;

import com.gg.common.Config;
import com.gg.thrift.ThriftClient;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Created by gaoge on 3/2/14.
 */
public class ExtractFeatureClient {

    public void process(){
        try{
            ZooKeeper zk = new ZooKeeper(Config.hostPort,Config.ZK_TIMEOUT,null);

            //get master node
            String masterNode = new String(zk.getData("/master",false,null));

            //get thrift port of master node
            String masterPort = new String(zk.getData("/jobs/"+masterNode,false,null));

            ThriftClient thriftClient = new ThriftClient();

            //get the selected job by calling master node
            String selectedJobHostPort = thriftClient.getHostPort("localhost:"+masterPort);
            System.out.println(selectedJobHostPort);

            //extract feature using the selected job
            thriftClient.extractFeature(selectedJobHostPort);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String args[]){
        new ExtractFeatureClient().process();
    }
}
