package com.cloudera.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.examples.WordCount;
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
import static org.junit.Assert.*;

/**
 *
 * @author aladdin
 */
public class WordCountJUnitTest {

    public WordCountJUnitTest() {
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
        "one two three",
        "one",
        "two",
        "i",
        "come to",
        "here"
    };
    
    private String[] mapOutputLineArr = {
        "one	1",
        "two	1",
        "three	1",
        "one	1",
        "two	1",
        "i	1",
        "come	1",
        "to	1",
        "here	1"
    };
    
    private String[] redInputLineArr = {
        "one	1",
        "one	1",
        "two	1",
        "two	1",
        "three	1",
        "i	1",
        "come	1",
        "to	1",
        "here	1"
    };
    
    private String[] redOutputLineArr = {
        "one	2",
        "two	2",
        "three	1",
        "i	1",
        "come	1",
        "to	1",
        "here	1"
    };
    
    private Mapper mapper = new WordCount.TokenizerMapper();
    private Reducer reducer = new WordCount.IntSumReducer();

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
        for (String line : this.redInputLineArr) {
            record = line.split("\t");
            key = record[0];
            value = record[1];
            if (key.equals(lastKey) == false) {
                if (lastKey.isEmpty() == false) {
                    reduceDriver.withInput(new Text(lastKey), valueList);
                    System.out.println(lastKey);
                }
                lastKey = key;
                valueList = new ArrayList<IntWritable>(4);
            }
            valueList.add(new IntWritable(Integer.parseInt(value)));
        }
        if (lastKey.isEmpty() == false) {
            reduceDriver.withInput(new Text(lastKey), valueList);
            System.out.println(lastKey);
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