package com.wolf.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author aladdin
 */
public class UserSoftAnalyzeMapredTest {

    public UserSoftAnalyzeMapredTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }
    private String[] mapInputLineArr = {
        "12374	012368002052600	1	12	3184	false	1207052155	2	1926265856	null	0",
        "12375	012368002052600	1	368	4710	false	1207172114	2	1926265856	null	0",
        "15989	012961000307800	1	11495	318	false	1207052209	2	107345920	null	0",
        "15991	012961000307800	1	13776	4639	false	1207052209	2	107345920	null	0",
        "16959	013024001131700	1	1729	3076	false	1207121238	2	801954304	null	0",
        "16960	013024001131700	1	11950	3097	false	1207121238	2	801954304	null	0"
    };
    private String[] mapOutputLineArr = {
        "ea800000	1",
        "ea800000	1",
        "a8000000	1",
        "a8000000	1",
        "a4000000	1",
        "a4000000	1"
    };
    private String[] redOutputLineArr = {
        "ea800000	2",
        "a8000000	2",
        "a4000000	2"
    };
    private Mapper mapper = new UserSoftAnalyzeMapred.MyMapper();
    private Reducer reducer = new UserSoftAnalyzeMapred.MyReducer();
    
//    @Test
    public void parsePart() {
        String part;
        String[] record;
        String imei;
        for (String line : mapInputLineArr) {
            record = line.split("\t");
            imei = record[1];
            part = PartitionUtils.getPartition(imei);
            System.out.println("imei to md5:" + part);
        }
    }

    @Test
    public void mapperTest() throws IOException {
        MapDriver mapDriver = new MapDriver(this.mapper);
        for (String line : this.mapInputLineArr) {
            mapDriver.withInput(NullWritable.get(), new Text(line));
        }
        //
        String[] record;
        for (String line : this.mapOutputLineArr) {
            record = line.split("\t");
            mapDriver.withOutput(new Text(record[0]), new IntWritable(Integer.parseInt(record[1])));
        }
        mapDriver.runTest();
    }
    
    @Test
    public void reducerTest() throws IOException {
        ReduceDriver reduceDriver = new ReduceDriver(this.reducer);
        String lastKey = "";
        String key;
        String value;
        String[] record;
        List<IntWritable> valueList = new ArrayList<IntWritable>(4);
        for (String line : this.mapOutputLineArr) {
            record = line.split("\t");
            key = record[0];
            value = record[1];
            if (key.equals(lastKey) == false) {
                if (lastKey.isEmpty() == false) {
                    reduceDriver.withInput(new Text(lastKey), valueList);
                }
                lastKey = key;
                valueList = new ArrayList<IntWritable>(4);
            }
            valueList.add(new IntWritable(Integer.parseInt(value)));
        }
        if (lastKey.isEmpty() == false) {
            reduceDriver.withInput(new Text(lastKey), valueList);
        }
        //
        for (String line : this.redOutputLineArr) {
            record = line.split("\t");
            key = record[0];
            value = record[1];
            reduceDriver.withOutput(new Text(key), new IntWritable(Integer.parseInt(value)));
        }
        reduceDriver.runTest();
    }
}