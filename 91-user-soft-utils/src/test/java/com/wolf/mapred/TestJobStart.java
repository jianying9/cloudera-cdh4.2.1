package com.wolf.mapred;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author aladdin
 */
public class TestJobStart extends AbstractJobStart {

    @Override
    public Job createJob() throws IOException {
        Job job = null;
//        Job job = new Job(this.getConf(), "test");
        return job;
    }

    @Override
    public String[] getValidateParameter() {
        String[] para = {};
        return para;
    }
}
