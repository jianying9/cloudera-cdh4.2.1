package com.cloudera.mapred;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 * @author aladdin
 */
public class HbaseMapredReadExample {

    public static class MyMapper extends TableMapper<Text, Text> {

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            // process data for the row from the Result instance.
        }
    }
}
