package com.cloudera.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.TestDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * create 'UserSoftTest',{NAME => 'INFO', VERSIONS => 1, BLOOMFILTER => 'ROWCOL', COMPRESSION => 'snappy', BLOCKCACHE => 'true' }, {NUMREGIONS => 512, SPLITALGO => 'HexStringSplit'}
 * @author aladdin
 */
public class UserSoftDayMapredTest {

    public UserSoftDayMapredTest() {
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
    
    private void initConfiguration(TestDriver testDriver) {
        Configuration config = HBaseConfiguration.create();
        Iterator<Entry<String, String>> iterator = config.iterator();
        Entry<String, String> entry;
        while(iterator.hasNext()) {
            entry = iterator.next();
            testDriver.getConfiguration().set(entry.getKey(), entry.getValue());
        }
        //
        testDriver.getConfiguration().set(UserSoftDayMapred.TABLE_NAME_PARA, "UserSoftTest");
    }
    
    private String[] mapInputLineArr = {
        "12374	012368002052600	1	12	3184	1207052155	1926265856	0",
        "12375	012368002052600	1	368	4710	1207172114	1926265856	0",
        "12375	012368002052600	1	368	4710	1207172115	1926265856	0",
        "15989	012961000307800	1	11495	318	1207052209	107345920	0",
        "15991	012961000307800	1	13776	4639	1207052209	107345920	0",
        "15991	012961000307800	1	13776	4639	1207052210	107345920	0",
        "16959	013024001131700	1	1729	3076	1207121238	801954304	0",
        "16960	013024001131700	1	11950	3097	1207121238	801954304	0",
        "16960	013024001131700	1	11950	3097	1207121239	801954304	0"
    };
    
    private String[] mapOutputLineArr = {
        "e54d_012368002052600	12_1207052155_1_3184",
        "e54d_012368002052600	368_1207172114_1_4710",
        "e54d_012368002052600	368_1207172115_1_4710",
        "b79d_012961000307800	11495_1207052209_1_318",
        "b79d_012961000307800	13776_1207052209_1_4639",
        "b79d_012961000307800	13776_1207052210_1_4639",
        "bf20_013024001131700	1729_1207121238_1_3076",
        "bf20_013024001131700	11950_1207121238_1_3097",
        "bf20_013024001131700	11950_1207121239_1_3097"
    };
    
    private String[] combinerOutputLineArr = {
        "e54d_012368002052600	368_1207172115_1_4710",
        "e54d_012368002052600	12_1207052155_1_3184",
        "b79d_012961000307800	13776_1207052210_1_4639",
        "b79d_012961000307800	11495_1207052209_1_318",
        "bf20_013024001131700	1729_1207121238_1_3076",
        "bf20_013024001131700	11950_1207121239_1_3097"
    };
    
    private Mapper mapper = new UserSoftDayMapred.MyMapper();
    private Reducer combiner = new UserSoftDayMapred.MyCombiner();
    private Reducer reducer = new UserSoftDayMapred.MyReducer();

    private Map<String, String> parseInputToOut(String line) {
        Map<String, String> resultMap = new HashMap<String, String>(2, 1);
        final StringBuilder columnBuilder = new StringBuilder(20);
        String[] record = line.split("\t");
        //构造输出key:part_imei
        String imei = record[1];
        String part = PartitionUtils.getPartitionHex(imei);
        final StringBuilder keyBuilder = new StringBuilder(40);
        keyBuilder.append(part).append('_').append(imei);
        resultMap.put("key", keyBuilder.toString());
        //
        //构造输出value:softId_gatherTime_platForm_softVersion_sourceId_isUninstalled
        String platForm = record[2];
        String softId = record[3];
        String softVersion = record[4];
        String gatherTime = record[6];
        columnBuilder.append(softId).append('_')
                .append(gatherTime).append('_')
                .append(platForm).append('_')
                .append(softVersion);
        resultMap.put("value", columnBuilder.toString());
        return resultMap;
    }
    
//    @Test
    public void createOutputTest() throws IOException {
        Map<String, String> outMap;
        for (String line : this.mapInputLineArr) {
            outMap = this.parseInputToOut(line);
            System.out.println(outMap.get("key") + "\t" + outMap.get("value"));
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
            mapDriver.withOutput(new Text(record[0]), new Text(record[1]));
        }
        mapDriver.runTest();
    }

    @Test
    public void combinerTest() throws IOException {
        ReduceDriver reduceDriver = new ReduceDriver(this.combiner);
        reduceDriver.getConfiguration();
        String lastKey = "";
        String key;
        String value;
        String[] record;
        List<Text> valueList = new ArrayList<Text>(4);
        for (String line : this.mapOutputLineArr) {
            record = line.split("\t");
            key = record[0];
            value = record[1];
            if (key.equals(lastKey) == false) {
                if (lastKey.isEmpty() == false) {
                    reduceDriver.withInput(new Text(lastKey), valueList);
                }
                lastKey = key;
                valueList = new ArrayList<Text>(4);
            }
            valueList.add(new Text(value));
        }
        if (lastKey.isEmpty() == false) {
            reduceDriver.withInput(new Text(lastKey), valueList);
        }
        //
        for (String line : this.combinerOutputLineArr) {
            record = line.split("\t");
            reduceDriver.withOutput(new Text(record[0]), new Text(record[1]));
        }
        reduceDriver.runTest();
    }
    
    @Test
    public void reducerTest() throws IOException {
        ReduceDriver reduceDriver = new ReduceDriver(this.reducer);
        this.initConfiguration(reduceDriver);
        String lastKey = "";
        String key;
        String value;
        String[] record;
        List<Text> valueList = new ArrayList<Text>(4);
        for (String line : this.mapOutputLineArr) {
            record = line.split("\t");
            key = record[0];
            value = record[1];
            if (key.equals(lastKey) == false) {
                if (lastKey.isEmpty() == false) {
                    reduceDriver.withInput(new Text(lastKey), valueList);
                }
                lastKey = key;
                valueList = new ArrayList<Text>(4);
            }
            valueList.add(new Text(value));
        }
        if (lastKey.isEmpty() == false) {
            reduceDriver.withInput(new Text(lastKey), valueList);
        }
        //
        reduceDriver.runTest();
    }
}