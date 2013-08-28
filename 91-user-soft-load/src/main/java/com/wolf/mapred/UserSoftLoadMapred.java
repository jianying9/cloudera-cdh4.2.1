package com.wolf.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
public class UserSoftLoadMapred {
    
    /**
     * 读取hdfs文件内容，输出固定key,value集合
     */
    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        
        private final Text newKey = new Text();
        private final Text newValue = new Text();
        private String strValue;
        private String[] record;
        private String imei;
        private String platForm;
        private String softId;
        private String softVersion;
        private String gatherTime;
        private String sourceId;
        private String isUninstalled;
        private String hashCode;
        private String p;
        private String part;
        private final StringBuilder keyBuilder = new StringBuilder(40);
        private final StringBuilder columnBuilder = new StringBuilder(20);

        /**
         * outKey:part_imei
         * outValue:softId_gatherTime_platForm_softVersion_sourceId_isUninstalled
         *
         * @param key
         * @param value: id imei platForm softId softVersion isUninstalled
         * gatherTime sourceId hashCode isHidden p
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            this.strValue = value.toString();
            this.record = this.strValue.split("\t");
            //判断数据是否完整
            if (this.record.length == 11) {
                //数据完整,取值
                this.imei = this.record[1];
                this.platForm = this.record[2];
                this.softId = this.record[3];
                this.softVersion = this.record[4];
                this.isUninstalled = this.record[5];
                if (this.isUninstalled.equals("true")) {
                    this.isUninstalled = "1";
                } else {
                    this.isUninstalled = "0";
                }
                this.gatherTime = this.record[6];
                this.sourceId = this.record[7];
                this.hashCode = this.record[8];
                this.p = this.record[10];
                this.part = PartitionUtils.getPartition(this.imei);
                //构造输出key:part_imei
                this.keyBuilder.append(this.part).append('_').append(this.imei);
                this.newKey.set(this.keyBuilder.toString());
                this.keyBuilder.setLength(0);
                //构造输出value:softId_gatherTime_platForm_softVersion_sourceId_isUninstalled_hashCode_p
                this.columnBuilder.append(this.softId).append('_')
                        .append(this.gatherTime).append('_')
                        .append(this.platForm).append('_')
                        .append(this.softVersion).append('_')
                        .append(this.sourceId).append('_')
                        .append(this.isUninstalled).append('_')
                        .append(this.hashCode).append('_')
                        .append(this.p);
                this.newValue.set(this.columnBuilder.toString());
                this.columnBuilder.setLength(0);
                context.write(newKey, newValue);
            }
        }
    }

    /**
     *
     */
    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        //hbase rowKey前缀

        private String rowKeyPrefix;
        //hbase table处理
//        private HTablePool hTablePool;
        private HTable hTable;
        private String tableName;
        private String lastParHex = "";
        private final byte[] columnFamily = Bytes.toBytes("INFO");
        //softId_gatherTime_platForm_softVersion_sourceId_isUninstalled_hashCode_p
        private final byte[] platFormByte = Bytes.toBytes("platForm");
        private final byte[] softVersionByte = Bytes.toBytes("softVersion");
        private final byte[] gatherTimeByte = Bytes.toBytes("gatherTime");
        private final byte[] sourceIdByte = Bytes.toBytes("sourceId");
        private final byte[] isUninstalledByte = Bytes.toBytes("isUninstalled");
        private final byte[] isHiddenByte = Bytes.toBytes("isHidden");
        private final byte[] hashCodeByte = Bytes.toBytes("hashCode");
        private final byte[] pByte = Bytes.toBytes("p");
        //等待写入hbase的put集合
        private final List<Put> putList = new ArrayList<Put>(100000);
        private final StringBuilder keyPrefixBuilder = new StringBuilder(40);
        //
        private double totalNum = 0;
        private long startTime = 0;
        
        private Put createInsertPut(String value) {
            //softId_gatherTime_platForm_softVersion_sourceId_isUninstalled_hashCode_p
            String[] record = value.split("_");
            String softId = record[0];
            String gatherTime = record[1];
            String platForm = record[2];
            String softVersion = record[3];
            String sourceId = record[4];
            String isUninstalled = record[5];
            String hashCode = record[6];
            String p = record[7];
            String isHidden = "0";
            String rowKey = this.rowKeyPrefix.concat(softId);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.setWriteToWAL(false);
            put.add(this.columnFamily, this.gatherTimeByte, Bytes.toBytes(gatherTime));
            put.add(this.columnFamily, this.platFormByte, Bytes.toBytes(platForm));
            put.add(this.columnFamily, this.softVersionByte, Bytes.toBytes(softVersion));
            put.add(this.columnFamily, this.sourceIdByte, Bytes.toBytes(sourceId));
            put.add(this.columnFamily, this.isUninstalledByte, Bytes.toBytes(isUninstalled));
            put.add(this.columnFamily, this.isHiddenByte, Bytes.toBytes(isHidden));
            put.add(this.columnFamily, this.hashCodeByte, Bytes.toBytes(hashCode));
            put.add(this.columnFamily, this.pByte, Bytes.toBytes(p));
            return put;
        }
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.tableName = context.getConfiguration().get(AbstractJobStart.TABLE_NAME_PARA);
//            this.hTablePool = new HTablePool(context.getConfiguration(), 1);
            this.startTime = System.currentTimeMillis();
            this.hTable = new HTable(context.getConfiguration(), this.tableName);
            this.hTable.setAutoFlush(false);
            this.hTable.setWriteBufferSize(536870912);
        }
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //业务处理
            String[] keyRecord = key.toString().split("_");
            String imei = keyRecord[1];
            String partHex = keyRecord[0];
            if (partHex.equals(this.lastParHex) == false) {
                if (this.putList.isEmpty() == false) {
                    this.totalNum += this.putList.size();
                    this.hTable.put(this.putList);
                    this.hTable.flushCommits();
                    long currentTime = System.currentTimeMillis();
                    int speed = (int) (this.totalNum * 1000 / (currentTime - this.startTime));
                    System.out.println("part:" + this.lastParHex + " size:" + this.putList.size() + " speed:" + speed + " row/s totalNum:" + (int) this.totalNum);
                    this.putList.clear();
                }
                this.lastParHex = partHex;
            }
            this.keyPrefixBuilder.append(partHex).append('_').append(imei).append('_');
            this.rowKeyPrefix = keyPrefixBuilder.toString();
            this.keyPrefixBuilder.setLength(0);
            //取值
            Put put;
            for (Text value : values) {
                //构造插入数据
                put = this.createInsertPut(value.toString());
                this.putList.add(put);
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (this.putList.isEmpty() == false) {
                this.hTable.put(this.putList);
                this.hTable.flushCommits();
                this.putList.clear();
            }
        }
    }
}
