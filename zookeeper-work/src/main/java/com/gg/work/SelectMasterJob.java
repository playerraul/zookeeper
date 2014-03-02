package com.gg.work;

import com.gg.common.Config;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

/**
 * Created by gaoge on 2/23/14.
 */
public class SelectMasterJob implements Watcher {
    ZooKeeper zk;
    String hostPort;

    SelectMasterJob(String hostPort){
        this.hostPort = hostPort;
    }

    void start() throws Exception{
        zk = new ZooKeeper(Config.hostPort,Config.ZK_TIMEOUT,this);
    }

    public void process(WatchedEvent e){
        System.out.println(e);
    }

    void selectMaster(){

        try{
            for(String w:zk.getChildren("/jobs",false)){
                if(isMasterExists()){
                    zk.setData("/master",w.getBytes(),-1);
                }else{
                    zk.create("/master",w.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                System.out.println("The master is :"+w);
                break;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    boolean isMasterExists(){
        boolean b = false;
        try{
            Stat stat = zk.exists("/master",false);
            if(stat != null){
                b = true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return b;
    }

    public static void main(String args[]) throws Exception{
        SelectMasterJob c = new SelectMasterJob(Config.hostPort);
        c.start();
        c.selectMaster();
    }

}
