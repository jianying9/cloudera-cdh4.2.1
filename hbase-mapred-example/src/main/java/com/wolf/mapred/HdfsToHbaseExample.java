package com.wolf.mapred;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author aladdin
 */
public class HdfsToHbaseExample {

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        private Text newKey = new Text();
        private Text newValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String strValue = value.toString();
            System.out.println("map:" + strValue);
            // autoId, imei, mac, softcode, pname, create_time, end_time, mark
            String[] record = strValue.split("\t");
            newKey.set(record[1] + "-" + record[2] + "@" + String.format("%09d", Integer.parseInt(record[0])));
            newValue.set(record[7] + "\t" + record[3] + "\t" + record[4] + "\t" + record[5] + "\t" + record[6]);

            context.write(newKey, newValue);
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String rowKey = key.toString().split("@")[0];
            System.out.println("reduce:" + rowKey);
            String[] rowKeyArr = rowKey.split("-");
            String imei = rowKeyArr[0];
            String mac = rowKeyArr[1];
            //
            Iterator<Text> iter = values.iterator();
            String[] strValue = iter.next().toString().split("\t");
            String softCode = strValue[1];
            String pname = strValue[2];
            String createTime = strValue[3];
            String endTime = strValue[4];
            Configuration config = context.getConfiguration();
            rowKey += Long.toString(System.currentTimeMillis());
            final byte[] rowKeyByte = Bytes.toBytes(rowKey);
            final Put put = new Put(rowKeyByte);
            final byte[] cfByte = Bytes.toBytes("INFO");
            put.add(cfByte, Bytes.toBytes("imei"), Bytes.toBytes(imei));
            put.add(cfByte, Bytes.toBytes("mac"), Bytes.toBytes(mac));
            put.add(cfByte, Bytes.toBytes("softCode"), Bytes.toBytes(softCode));
            put.add(cfByte, Bytes.toBytes("pname"), Bytes.toBytes(pname));
            put.add(cfByte, Bytes.toBytes("createTime"), Bytes.toBytes(createTime));
            put.add(cfByte, Bytes.toBytes("endTime"), Bytes.toBytes(endTime));
            HTable hTable = new HTable(config, "Installed_App_List");
            hTable.put(put);
        }
    }
}
