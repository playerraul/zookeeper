import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOError;
import java.io.IOException;

/**
 * Created by gaoge on 2/16/14.
 */
public class Master implements Watcher {
    ZooKeeper zk;
    String hostPort;

    Master(String hostPort){
        this.hostPort = hostPort;
    }

    void startZK(){
        try{
            zk = new ZooKeeper(hostPort,15000,this);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent e){
        System.out.println(e);
    }

    public static void main(String args[]) throws Exception{
        Master m = new Master(args[0]);
        m.startZK();

        Thread.sleep(60000);
    }
}
