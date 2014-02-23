package com.gg.test;

import com.gg.common.Config;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Date;

/**
 * Created by gaoge on 2/23/14.
 */
public class StatChange implements Watcher {

    ZooKeeper zk;
    String hostPort;

    StatChange(String hostPort){
        this.hostPort = hostPort;
    }

    void start() throws Exception{
        zk = new ZooKeeper(Config.hostPort,15000,this);

        zk.create("/master","haha".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    void stopZK() throws Exception{zk.close();}


    public void process(WatchedEvent e){
        System.out.println(e);
    }

    public static void main(String args[]) throws Exception{
        StatChange z = new StatChange(Config.hostPort);
        z.start();

        z.masterExists();

        Thread.sleep(10000);
        z.stopZK();


        Thread.sleep(20000);
    }

    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback(){
        public void processResult(int rc,String path,Object ctx,Stat stat){
            System.out.println("there");
            System.out.println("code:"+KeeperException.Code.get(rc));
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
//                    checkMaster();
                    break;
                case OK:
                    System.out.println("ok");
                    break;
                case NODEEXISTS:
                    masterExists();
                    break;
                default:
                    System.out.println("somthing wrong");
            }
        }
    };

    void masterExists(){
        System.out.println("here");
        zk.exists("/master",masterExistsWatcher,masterExistsCallback,null);
    }

    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if(watchedEvent.getType() == Event.EventType.NodeDeleted){
                System.out.println("deleted!");
            }
        }
    };
}
