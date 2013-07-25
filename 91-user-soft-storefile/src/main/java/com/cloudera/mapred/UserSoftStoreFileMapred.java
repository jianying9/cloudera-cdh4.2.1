package com.cloudera.mapred;

import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author aladdin
 */
public class UserSoftStoreFileMapred {
    
    public static final String TABLE_NAME_PARA = "-Dmapred.tableName";

    /**
     * 读取hdfs文件内容，输出固定key,value集合
     */
    public static class MyMapper extends Mapper<Object, Text, ImmutableBytesWritable, KeyValue> {

        private String strValue;
        private String[] record;
        private String imei;
        private String platForm;
        private String softId;
        private String softVersion;
        private String gatherTime;
        private String sourceId;
        private final String isHidden = "0";
        private String isUninstalled;
        private String hashCode;
        private String p;
        private int part;
        private String partHex;
        private String rowKey;
        private byte[] rowKeyByte;
        private final StringBuilder keyBuilder = new StringBuilder(40);
        private ImmutableBytesWritable outkey;
        private KeyValue outValue;
        
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
        private final long time = System.currentTimeMillis();

        /**
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
                this.partHex = String.format("%04x", part);
                //构造输出key:part_imei
                this.keyBuilder.append(this.partHex).append('_').append(this.imei).append('_').append(this.softId);
                this.rowKey = this.keyBuilder.toString();
                this.keyBuilder.setLength(0);
                this.rowKeyByte = Bytes.toBytes(this.rowKey);
                this.outkey = new ImmutableBytesWritable(this.rowKeyByte);
                //
                this.outValue = new KeyValue(this.rowKeyByte, this.columnFamily, this.platFormByte, this.time, Bytes.toBytes(this.platForm));
                context.write(this.outkey, this.outValue);
                this.outValue = new KeyValue(this.rowKeyByte, this.columnFamily, this.softVersionByte, this.time, Bytes.toBytes(this.softVersion));
                context.write(this.outkey, this.outValue);
                this.outValue = new KeyValue(this.rowKeyByte, this.columnFamily, this.gatherTimeByte, this.time, Bytes.toBytes(this.gatherTime));
                context.write(this.outkey, this.outValue);
                this.outValue = new KeyValue(this.rowKeyByte, this.columnFamily, this.sourceIdByte, this.time, Bytes.toBytes(this.sourceId));
                context.write(this.outkey, this.outValue);
                this.outValue = new KeyValue(this.rowKeyByte, this.columnFamily, this.isUninstalledByte, this.time, Bytes.toBytes(this.isUninstalled));
                context.write(this.outkey, this.outValue);
                this.outValue = new KeyValue(this.rowKeyByte, this.columnFamily, this.isHiddenByte, this.time, Bytes.toBytes(this.isHidden));
                context.write(this.outkey, this.outValue);
                this.outValue = new KeyValue(this.rowKeyByte, this.columnFamily, this.hashCodeByte, this.time, Bytes.toBytes(this.hashCode));
                context.write(this.outkey, this.outValue);
                this.outValue = new KeyValue(this.rowKeyByte, this.columnFamily, this.pByte, this.time, Bytes.toBytes(this.p));
                context.write(this.outkey, this.outValue);
            }
        }
    }
}
