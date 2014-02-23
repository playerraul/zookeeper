package com.gg.test;

import com.gg.common.Config;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

/**
 * Created by gaoge on 2/23/14.
 */
public class RegisterWorker implements Watcher {
//    private static final Logger LOG = LoggerFactory.getLogger(RegisterWorker.class);

    ZooKeeper zk;
    static String hostPort;
    static String serverId;

    RegisterWorker(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException{
        zk = new ZooKeeper(hostPort,35000,this);
    }

    public void process(WatchedEvent e){
        System.out.println(e.toString() + ", " + hostPort);
    }

    void register(){
        zk.create("/workers/"+serverId,"idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,createWorkerCallback,null);
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback(){
        public void processResult(int rc,String path,Object ctx,String name){
            System.out.println("exception code:"+KeeperException.Code.get(rc));
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    System.out.println("register ok:" + serverId);
                    break;
                case NODEEXISTS:
                    System.out.println("already registered:" + serverId);
                    break;
                default:
                    System.out.println("something wrong" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    public static void main(String args[]) throws Exception{

       serverId = Integer.toString(new Random().nextInt());

        RegisterWorker w = new RegisterWorker(Config.hostPort);
        w.startZK();

        w.register();

        //add listener to watch if deleting
        w.workerExists();

        Thread.sleep(90000);
    }


    AsyncCallback.StatCallback workerExistsCallback = new AsyncCallback.StatCallback(){
        public void processResult(int rc,String path,Object ctx,Stat stat){
            System.out.println("code:"+KeeperException.Code.get(rc));
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    break;
                case OK:
                    System.out.println("ok");
                    break;
                case NODEEXISTS:
                    workerExists();
                    break;
                default:
                    System.out.println("somthing wrong");
            }
        }
    };

    void workerExists(){
        zk.exists("/workers/"+serverId,workerExistsWatcher,workerExistsCallback,null);
    }

    Watcher workerExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if(watchedEvent.getType() == Event.EventType.NodeDeleted){
                System.out.println(watchedEvent.getPath());
                System.out.println("feature job deleted!");
                if(watchedEvent.getPath().endsWith(getMasterNode())){
                    System.out.println("master feature job is deleted!");
                    selectMaster();
                }
            }
        }
    };

    String getMasterNode(){
        try{
            return new String(zk.getData("/master",false,null));
        }catch (Exception e){
            return "";
        }
    }

    void selectMaster(){
        try{
            for(String w:zk.getChildren("/workers",false)){
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
}
