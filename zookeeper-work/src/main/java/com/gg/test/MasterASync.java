package com.gg.test;

import com.gg.common.Config;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Random;

/**
 * Created by gaoge on 2/16/14.
 */
public class MasterASync implements Watcher {
    ZooKeeper zk;

    private static String serverId;

    private static boolean isLeader = false;
    private static String hostPort;
//    private static String hostPort = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    MasterASync(String hostPort){
        this.hostPort = hostPort;
    }


    void checkMaster(){
        zk.getData("/master",false,masterCheckCallback,null);
    }

    void runForMaster(){
        zk.create("/master",serverId.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL,masterCreateCallback,null);
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
        MasterASync m = new MasterASync(Config.hostPort);
        serverId = Long.toString(new Random().nextLong());
        m.startZK();



        /*
        m.runForMaster();

        //if comment this code, the client will disconnected after some seconds!!!
        m.stopZK();
        */
//        m.bootstrap();

    }

    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            System.out.println("I am " + (isLeader ?"":"not")+"the leader");
        }
    };

    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback(){
        public void processResult(int rc,String path,Object ctx,byte[] data,Stat stat){
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    runForMaster();
                    return;
            }
        }
    };



    // setting up metadata
    public void bootstrap(){
        createParent("/workers",new byte[0]);
        createParent("/assign",new byte[0]);
        createParent("/tasks",new byte[0]);
        createParent("/status",new byte[0]);
    }

    void createParent(String path, byte[] data){
        zk.create(path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,createParentCallback,data);
    }

    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback(){
        public void processResult(int rc,String path,Object ctx,String name){
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    createParent(path,(byte[])ctx);
                    break;
                case OK:
                    System.out.println("parent created");
                    break;
                case NODEEXISTS:
                    System.out.println("parent already registered"+path);
                    break;
                default:
                    System.out.println("something wrong"+KeeperException.create(KeeperException.Code.get(rc),path));
            }
        }
    };
}
