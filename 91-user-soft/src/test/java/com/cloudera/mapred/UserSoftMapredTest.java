package com.cloudera.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author aladdin
 */
public class UserSoftMapredTest {
    
    public UserSoftMapredTest() {
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
    
    private String[] inputLineArr = {
        "12374	012368002052600	1	12	3184	false	1207052155	2	1926265856	null	0",
        "12375	012368002052600	1	368	4710	false	1207172114	2	1926265856	null	0",
        "12375	012368002052600	1	368	4710	false	1207172115	2	1926265856	null	0",
        "15989	012961000307800	1	11495	318	false	1207052209	2	107345920	null	0",
        "15991	012961000307800	1	13776	4639	false	1207052209	2	107345920	null	0",
        "15991	012961000307800	1	13776	4639	false	1207052210	2	107345920	null	0",
        "16959	013024001131700	1	1729	3076	false	1207121238	2	801954304	null	0",
        "16960	013024001131700	1	11950	3097	false	1207121238	2	801954304	null	0",
        "16960	013024001131700	1	11950	3097	false	1207121239	2	801954304	null	0"
    };
    private Mapper mapper = new UserSoftMapred.MyMapper();
    
    private List<List<Text>> createOutput() {
        List<List<Text>> resultList = new ArrayList<List<Text>>(this.inputLineArr.length);
        String[] record;
        String imei;
        String platForm;
        String softId;
        String softVersion;
        String gatherTime;
        String sourceId;
        String isUninstalled;
        String part;
        Text newKey;
        Text newValue;
        List<Text> lineList;
        final StringBuilder keyBuilder = new StringBuilder(40);
        final StringBuilder columnBuilder = new StringBuilder(20);
        for (String line : this.inputLineArr) {
            record = line.split("\t");
            imei = record[1];
            //构造输出key:part_imei
            part = PartitionUtils.getPartitionHex(imei);
            keyBuilder.append(part).append('_').append(imei);
            newKey = new Text();
            newKey.set(keyBuilder.toString());
            keyBuilder.setLength(0);
            //
            platForm = record[2];
            softId = record[3];
            softVersion = record[4];
            gatherTime = record[6];
            sourceId = record[7];
            isUninstalled = record[10];
            //构造输出value:softId_gatherTime_platForm_softVersion_sourceId_isUninstalled
            columnBuilder.append(softId).append('_')
                    .append(gatherTime).append('_')
                    .append(platForm).append('_')
                    .append(softVersion).append('_')
                    .append(sourceId).append('_')
                    .append(isUninstalled);
            newValue = new Text();
            newValue.set(columnBuilder.toString());
            columnBuilder.setLength(0);
            //
            lineList = new ArrayList<Text>(2);
            lineList.add(newKey);
            lineList.add(newValue);
            resultList.add(lineList);
        }
        return resultList;
    }
    
    @Test
    public void mapperTest() throws IOException {
        MapDriver mapDriver = new MapDriver(this.mapper);
        for (String line : this.inputLineArr) {
            mapDriver.withInput(NullWritable.get(), new Text(line));
        }
        //
        List<List<Text>> outputList = this.createOutput();
        for (List<Text> lineList : outputList) {
            mapDriver.withOutput(lineList.get(0), lineList.get(1));
        }
        mapDriver.runTest();
    }
}