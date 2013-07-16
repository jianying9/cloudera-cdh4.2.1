package com.cloudera.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author aladdin
 */
public class UserSoftMapred {

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
        private String part;
        private final StringBuilder keyBuilder = new StringBuilder(40);
        private final StringBuilder columnBuilder = new StringBuilder(20);

        /**
         * outKey:part_imei
         * outValue:softId_gatherTime_platForm_softVersion_sourceId_isUninstalled
         *
         * @param key
         * @param value: id imei platForm softId softVersion "" gatherTime
         * sourceId hashCode isHidden isUninstalled
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
                this.gatherTime = this.record[6];
                this.sourceId = this.record[7];
                this.isUninstalled = this.record[10];
                this.part = PartitionUtils.getPartitionHex(imei);
                //构造输出key:part_imei
                this.keyBuilder.append(this.part).append('_').append(this.imei);
                this.newKey.set(this.keyBuilder.toString());
                this.keyBuilder.setLength(0);
                //构造输出value:softId_gatherTime_platForm_softVersion_sourceId_isUninstalled
                this.columnBuilder.append(this.softId).append('_')
                        .append(this.gatherTime).append('_')
                        .append(this.platForm).append('_')
                        .append(this.softVersion).append('_')
                        .append(this.sourceId).append('_')
                        .append(this.isUninstalled);
                this.newValue.set(this.columnBuilder.toString());
                this.columnBuilder.setLength(0);
                context.write(newKey, newValue);
            }
        }
    }

    /**
     * 对相同的part_imei下的value进行排序，结果根据softId_gatherTime从大到小排列
     */
    public static class ValueComparator implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            return o2.compareTo(o1);
        }
    }

    /**
     * 对map输出的结果去重复，相同的part_imei_softId,取gatherTime最大的记录 以减少向reducer传输的数据大小，减少io
     * inputKey:part_imei
     * inputValue:softId_gatherTime_platForm_softVersion_sourceId_isUninstalled
     */
    public static class MyCombiner extends Reducer<Text, Text, Text, Text> {

        private final Text newValue = new Text();
        private final List<String> valueList = new ArrayList<String>(300);
        private final ValueComparator valueComparator = new ValueComparator();
        private String lastSoftId;
        private String softId;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //取值
            for (Text value : values) {
                this.valueList.add(value.toString());
            }
            //排序,结果根据softId_gatherTime从大到小排列
            Collections.sort(this.valueList, this.valueComparator);
            //只保留每个softId中gatherTime最大的记录
            this.lastSoftId = "";
            for (String value : this.valueList) {
                this.softId = value.substring(0, value.indexOf("_"));
                if (this.softId.equals(this.lastSoftId) == false) {
                    //排序后，集合中首次出现的即为最大值
                    this.lastSoftId = this.softId;
                    this.newValue.set(value);
                    context.write(key, this.newValue);
                }
            }
            //清空集合
            this.valueList.clear();
        }
    }

    /**
     * 将map输出的结果根据part进行分类，相同的part的数据分配到同一个reducer中
     */
    public static class MyPartitioner extends Partitioner<Text, Text> {

        private String part;

        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            this.part = key.toString().substring(0, 4);
            return (this.part.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    /**
     *
     */
    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        //part_imei所有数据集合
        private final List<String> allValueList = new ArrayList<String>(300);
        //hbase rowKey前缀
        private String rowKeyPrefix;
        //part_imei去重后数据集合
        private final List<String> valueList = new ArrayList<String>(100);
        //排序处理对象
        private final ValueComparator valueComparator = new ValueComparator();
        //hbase table处理
        private final HTablePool hTablePool;
        private final String tableName;
        private final byte[] columnFamily = Bytes.toBytes("info");
        //softId_gatherTime_platForm_softVersion_sourceId_isUninstalled
        private final byte[] platFormByte = Bytes.toBytes("platForm");
        private final byte[] softVersionByte = Bytes.toBytes("softVersion");
        private final byte[] gatherTimeByte = Bytes.toBytes("gatherTime");
        private final byte[] sourceIdByte = Bytes.toBytes("sourceId");
        private final byte[] isUninstalledByte = Bytes.toBytes("isUninstalled");
        //等待写入hbase的put集合
        private final List<Put> putList = new ArrayList<Put>(10000);
        //hbase已有part_imei数据缓存
        private final List<String> softIdList = new ArrayList<String>(200);

        public MyReducer() {
            Configuration config = HBaseConfiguration.create();
            this.hTablePool = new HTablePool(config, 1);
            this.tableName = "Test";
        }

        /**
         * 获取一个hbase的连接对象
         */
        private HTableInterface getHTable(String tableName) {
            return this.hTablePool.getTable(tableName);
        }

        /**
         * 获取前缀为rowKeyPrefix的数据，并将rowKey放入rowKeySet
         */
        private void inquireRowKeyByPrefix() {
            ResultScanner rs = null;
            Scan scan = new Scan();
            scan.setBatch(500);
            scan.setMaxVersions();
            scan.setStartRow(Bytes.toBytes(this.rowKeyPrefix));
            scan.setStopRow(Bytes.toBytes(this.rowKeyPrefix.concat("_")));
            scan.addColumn(this.columnFamily, this.gatherTimeByte);
            HTableInterface hTableInterface = this.getHTable(this.tableName);
            try {
                byte[] rowKey;
                String softId;
                rs = hTableInterface.getScanner(scan);
                Result result = rs.next();
                while (result != null) {
                    rowKey = result.getRow();
                    softId = Bytes.toString(rowKey);
                    softId = softId.substring(softId.lastIndexOf("_"));
                    this.softIdList.add(softId);
                    result = rs.next();
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }

        private Put createInsertPut(String value) {
            //softId_gatherTime_platForm_softVersion_sourceId_isUninstalled
            String[] record = value.split("_");
            String softId = record[0];
            String gatherTime = record[1];
            String platForm = record[2];
            String softVersion = record[3];
            String sourceId = record[4];
            String isUninstalled = record[5];
            String rowKey = this.rowKeyPrefix.concat(softId);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(this.columnFamily, this.gatherTimeByte, Bytes.toBytes(gatherTime));
            put.add(this.columnFamily, this.platFormByte, Bytes.toBytes(platForm));
            put.add(this.columnFamily, this.softVersionByte, Bytes.toBytes(softVersion));
            put.add(this.columnFamily, this.sourceIdByte, Bytes.toBytes(sourceId));
            put.add(this.columnFamily, this.isUninstalledByte, Bytes.toBytes(isUninstalled));
            return put;
        }

        private Put createUpdatePut(String value) {
            //softId_gatherTime_platForm_softVersion_sourceId_isUninstalled
            String[] record = value.split("_");
            String softId = record[0];
            String gatherTime = record[1];
            String softVersion = record[3];
            String isUninstalled = record[5];
            String rowKey = this.rowKeyPrefix.concat(softId);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(this.columnFamily, this.gatherTimeByte, Bytes.toBytes(gatherTime));
            put.add(this.columnFamily, this.softVersionByte, Bytes.toBytes(softVersion));
            put.add(this.columnFamily, this.isUninstalledByte, Bytes.toBytes(isUninstalled));
            return put;
        }

        private Put createDeletePut(String softId) {
            String isUninstalled = "1";
            String rowKey = this.rowKeyPrefix.concat(softId);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(this.columnFamily, this.isUninstalledByte, Bytes.toBytes(isUninstalled));
            return put;
        }

        //业务处理
        private void execute() {
            //从hbase获取前缀为rowKeyPrefix的数据
            this.inquireRowKeyByPrefix();
            //比较输入数据和缓存数据
            String softId;
            Put put;
            for (String value : this.valueList) {
                softId = value.substring(0, value.indexOf("_"));
                if (this.softIdList.contains(softId)) {
                    //hbase已经存在,构造更新put
                    put = this.createUpdatePut(value);
                    //将softId从softIdList中移除
                    this.softIdList.remove(softId);
                } else {
                    //hbase不存在，构造插入put
                    put = this.createInsertPut(value);
                }
                this.putList.add(put);
            }
            //如果softIdList集合size不为0,则剩下的softId则认为已经不用户删除
            for (String deleteSoftId : this.softIdList) {
                //够造标记逻辑删除put
                put = this.createDeletePut(deleteSoftId);
                this.putList.add(put);
            }
            //执行更新动作
            if (this.putList.size() >= 10000) {
                HTableInterface hTableInterface = this.hTablePool.getTable(this.tableName);
                try {
                    hTableInterface.put(this.putList);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
                this.putList.clear();
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //reducer端数据去重,相同的part_imei_softId,取gatherTime最大的记录 以减少向reducer传输的数据大小
            //取值
            for (Text value : values) {
                this.allValueList.add(value.toString());
            }
            //排序,结果根据softId_gatherTime从大到小排列
            Collections.sort(this.allValueList, this.valueComparator);
            //只保留每个softId中gatherTime最大的记录
            String lastSoftId = "";
            String softId;
            for (String value : this.allValueList) {
                softId = value.substring(0, value.indexOf("_"));
                if (softId.equals(lastSoftId) == false) {
                    //排序后，集合中首次出现的即为最大值
                    lastSoftId = softId;
                    this.valueList.add(value);
                }
            }
            //清除原始数据集合
            this.allValueList.clear();
            //业务处理
            this.rowKeyPrefix = key.toString().concat("_");
            this.execute();
            //清空业务数据集合
            this.valueList.clear();
            this.softIdList.clear();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (this.putList.isEmpty() == false) {
                HTableInterface hTableInterface = this.hTablePool.getTable(this.tableName);
                try {
                    hTableInterface.put(this.putList);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
                this.putList.clear();
            }
        }
    }
}
