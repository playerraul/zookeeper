package com.gg.thrift;

import com.gg.thrift.FeatureThriftService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gaoge on 3/2/14.
 */
public class ThriftClient {
    static Map<String, FeatureThriftService.Client> clientMap = new HashMap<String, FeatureThriftService.Client>();
    private static Object sync = new Object();

    private FeatureThriftService.Client getClient(String hostPort) {
        FeatureThriftService.Client client = clientMap.get(hostPort);

        if (client == null) {
            synchronized (sync) {
                if (client == null) {
                    client = connect(hostPort);
                    clientMap.put(hostPort, client);
                }
            }
        }
        return client;
    }

    private FeatureThriftService.Client connect(String hostPort) {
        TSocket tSocket = new TSocket(hostPort.split(":")[0], Integer.valueOf(hostPort.split(":")[1]));
        try {
            tSocket.open();
        } catch (TTransportException e) {
            e.printStackTrace();
        }

        TProtocol protocol = new TBinaryProtocol(tSocket);
        FeatureThriftService.Client client = new FeatureThriftService.Client(protocol);
        return client;
    }

    public void ping(String hostPort) throws TException {
        FeatureThriftService.Client client = getClient(hostPort);
        client.ping();
    }

    public String getHostPort(String hostPort) {

        FeatureThriftService.Client client = getClient(hostPort);
        try {
            return client.getHostPort();
        } catch (TException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void extractFeature(String hostPort) {
        FeatureThriftService.Client client = getClient(hostPort);
        try {
            client.extractFeature();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
