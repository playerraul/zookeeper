package com.gg.work;

import com.gg.common.Config;
import org.junit.Test;

import java.util.Random;

/**
 * Created by gaoge on 3/2/14.
 */
public class ExtractFeatureTest {

    @Test
    public void extractFeatureTest(){
        ExtractFeatureClient c = new ExtractFeatureClient();
        c.process();
    }

    @Test
    public void selectMasterJobTest() throws Exception {
        SelectMasterJob c = new SelectMasterJob(Config.hostPort);
        c.start();
        c.selectMaster();
    }

    @Test
    public void registerJobTest() throws Exception {
        String jobId = Integer.toString(new Random().nextInt());

        RegisterJob w = new RegisterJob(Config.hostPort);
        w.startZK();

        w.register();

        w.jobExists();

//        w.startThriftServer();
    }
}
