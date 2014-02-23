package com.gg.test;

import com.gg.common.Config;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Random;

/**
 * Created by gaoge on 2/16/14.
 */
public class MasterSync implements Watcher {
    ZooKeeper zk;

    private static String serverId;

    private static boolean isLeader = false;
    private static String hostPort;
//    private static String hostPort = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    MasterSync(String hostPort){
        this.hostPort = hostPort;
    }

    boolean checkMaster(){
        while(true){
            try{
                Stat stat = new Stat();
                byte data[] = zk.getData("/master",false,stat);
                isLeader = new String(data).equals(serverId);
                System.out.println("node name:"+new String(data));
                return true;
            }catch(KeeperException.NoNodeException e){
                return false;
            }catch (InterruptedException e){

            }catch (KeeperException e){

            }
        }
    }

    void runForMaster() throws InterruptedException{
        while (true){
            try{
                zk.create("/master",serverId.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                System.out.println("create master node");
                isLeader = true;
                break;
            }catch(KeeperException.NodeExistsException e){
                System.out.println("node exists");
                isLeader = false;
                break;
            }catch (KeeperException e){
                e.printStackTrace();
            }
            if(checkMaster()) break;

        }
    }

    void startZK(){
        try{
            zk = new ZooKeeper(hostPort,15000,this);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    void stopZK() throws Exception{zk.close();}

    public void process(WatchedEvent e){
        System.out.println(e);
    }

    public static void main(String args[]) throws Exception{
        Random r = new Random();
        serverId = Long.toString(r.nextLong());

        MasterSync m = new MasterSync(Config.hostPort);

        m.startZK();

        m.runForMaster();
        if(isLeader){
            System.out.println("I am the leader");
//            Thread.sleep(10000);
        }else{
            System.out.println("some else is the leader");
        }

        //if comment this code, the client will disconnected after some seconds!!!
        m.stopZK();

//        System.out.println(m.getZk().getState());
//        m.getZk().get
//        Thread.sleep(60000);
    }

    ZooKeeper getZk(){
        return zk;
    }
}
