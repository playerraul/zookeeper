package com.gg.thrift;

import com.gg.common.Config;
import org.apache.thrift.TException;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.util.Random;

/**
 * Created by gaoge on 3/2/14.
 */
public class FeatureThriftServiceHandler implements FeatureThriftService.Iface {
    @Override
    public void extractFeature() throws TException {
        System.out.println("Extract feature successfully!");
    }

    @Override
    public String getHostPort() throws TException {
        try{
            ZooKeeper zk = new ZooKeeper(Config.hostPort,Config.ZK_TIMEOUT,null);
            List<String> jobs = zk.getChildren("/jobs",true);
            int index = new Random().nextInt(jobs.size());
            return "localhost:"+new String(zk.getData("/jobs/"+jobs.get(index),false,null));
        }catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void ping() throws TException {
        System.out.println("ping");
    }
}
