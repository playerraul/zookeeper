package com.gg.thrift;

import com.gg.common.Config;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Created by gaoge on 3/2/14.
 */
public class ThriftServer {

    TServer server;

    public void start(int port,String jobId) throws TTransportException, IOException, KeeperException, InterruptedException {

        TServerTransport serverTransport = new TServerSocket(port);
        TProcessor processor = new FeatureThriftService.Processor<FeatureThriftServiceHandler>(new FeatureThriftServiceHandler());

        server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

        System.out.println("start thrift server");
        new ZooKeeper(Config.hostPort,Config.ZK_TIMEOUT,null).setData("/jobs/" + jobId, Integer.toString(port).getBytes(), -1);
        server.serve();

    }

    public void stop() {
        if (server != null) {
            server.stop();
        }
    }

    public static void main(String[] args) throws TTransportException {
//        new ThriftServer().start(9090);
    }
}
