package com.gg.work;

import com.gg.common.Config;
import com.gg.sample.AdminClient;
import org.junit.Test;

/**
 * Created by gaoge on 3/2/14.
 */
public class AdminClientTest {
    @Test
    public void test() throws Exception {
        AdminClient c = new AdminClient(Config.hostPort);
        c.start();
        c.listState();
    }
}
