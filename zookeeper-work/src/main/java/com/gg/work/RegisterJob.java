package com.gg.work;

import com.gg.common.Config;
import com.gg.thrift.ThriftServer;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Created by gaoge on 2/23/14.
 */
public class RegisterJob implements Watcher {

    ZooKeeper zk;
    static String hostPort;
    static String jobId;

    RegisterJob(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException{
        zk = new ZooKeeper(hostPort,Config.ZK_TIMEOUT,this);
    }

    public void process(WatchedEvent e){
        System.out.println("watched event:"+e.getType());
        try {
            List<String> children = zk.getChildren("/jobs",true);

            //if master is not exists
            if(!children.contains(getMasterNode())){
                selectMaster();
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }

    void register(){
        zk.create("/jobs/"+ jobId,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,createJobCallback,null);
    }

    AsyncCallback.StringCallback createJobCallback = new AsyncCallback.StringCallback(){
        public void processResult(int rc,String path,Object ctx,String name){
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    System.out.println("register ok:" + jobId);
                    break;
                case NODEEXISTS:
                    System.out.println("already registered:" + jobId);
                    break;
                default:
                    System.out.println("something wrong" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


    void jobExists() throws KeeperException, InterruptedException {
        zk.getChildren("/jobs",true);
    }

    String getMasterNode(){
        try{
            return new String(zk.getData("/master",false,null));
        }catch (Exception e){
            return "";
        }
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

    public void startThriftServer() throws KeeperException, InterruptedException, IOException {
        //simple way to set port for thrift server
        int i;
        for (i=9001;i<9999;i++){
            System.out.println(i);
            try {
                new ThriftServer().start(i, jobId);
                break;
            } catch (TTransportException e) {
                System.out.println("Thrift port '" + i + "' already in use!");
            }
        }
    }

    public static void main(String args[]) throws Exception{

        jobId = Integer.toString(new Random().nextInt());

        RegisterJob w = new RegisterJob(Config.hostPort);
        w.startZK();

        w.register();

        //add listener to watch jobs
        w.jobExists();

        w.startThriftServer();
    }
}
