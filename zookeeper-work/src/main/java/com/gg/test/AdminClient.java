package com.gg.test;

import com.gg.common.Config;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Date;

/**
 * Created by gaoge on 2/23/14.
 */
public class AdminClient implements Watcher {

    ZooKeeper zk;
    String hostPort;

    AdminClient(String hostPort){
        this.hostPort = hostPort;
    }

    void start() throws Exception{
        zk = new ZooKeeper(Config.hostPort,15000,this);
    }

    void listState() throws Exception{
        try{
            Stat stat = new Stat();
            byte masterData[] = zk.getData("/master",false,stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("master: "+new String(masterData)+" since "+startDate);
        }catch (KeeperException.NoNodeException e){
            System.out.println("no master");
        }

        System.out.println("workers:");
        for(String w:zk.getChildren("/workers",false)){
            byte data[] = zk.getData("/workers/"+w, false, null);
            String state = new String(data);
            System.out.println("\t"+w+": "+state);
        }

        System.out.println("tasks:");
        for(String t: zk.getChildren("/assign",false)){
            System.out.println("\t"+t);
        }
    }

    public void process(WatchedEvent e){
        System.out.println(e);
    }

    public static void main(String args[]) throws Exception{
        AdminClient c = new AdminClient(Config.hostPort);
        c.start();
        c.listState();
    }
}
