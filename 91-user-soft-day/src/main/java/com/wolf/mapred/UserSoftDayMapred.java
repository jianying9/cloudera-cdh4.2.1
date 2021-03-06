package com.wolf.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author aladdin
 */
public class UserSoftDayMapred {

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
        private String hashCode;
        private String p;
        private String part;
        private final StringBuilder keyBuilder = new StringBuilder(40);
        private final StringBuilder columnBuilder = new StringBuilder(20);

        /**
         * outKey:part_imei
         * outValue:softId_gatherTime_platForm_softVersion_hashCode_p
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
            if (this.record.length == 8) {
                //数据完整,取值
                this.imei = this.record[1];
                this.platForm = this.record[2];
                this.softId = this.record[3];
                this.softVersion = this.record[4];
                this.gatherTime = this.record[5];
                this.hashCode = this.record[6];
                this.p = this.record[7];
                this.part = PartitionUtils.getPartition(this.imei);
                //构造输出key:part_imei
                this.keyBuilder.append(this.part).append('_').append(this.imei);
                this.newKey.set(this.keyBuilder.toString());
                this.keyBuilder.setLength(0);
                //构造输出value:softId_gatherTime_platForm_softVersion_hashCode_p
                this.columnBuilder.append(this.softId).append('_')
                        .append(this.gatherTime).append('_')
                        .append(this.platForm).append('_')
                        .append(this.softVersion).append('_')
                        .append(this.hashCode).append('_')
                        .append(this.p);
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
     * inputValue:softId_gatherTime_platForm_softVersion_hashCode_p
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
     *
     */
    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        //part_imei所有数据集合
        private final List<String> allValueList = new ArrayList<String>(50000);
        //hbase rowKey前缀
        private String rowKeyPrefix;
        private String lastPartHex = "";
        //part_imei去重后数据集合
        private final List<String> valueList = new ArrayList<String>(50000);
        //排序处理对象
        private final ValueComparator valueComparator = new ValueComparator();
        //hbase table处理
        private HTable hTable;
        private String tableName;
//        private Filter filter = new FirstKeyOnlyFilter();
        private final byte[] columnFamily = Bytes.toBytes("INFO");
        //softId_gatherTime_platForm_softVersion_sourceId_isUninstalled
        private final byte[] platFormByte = Bytes.toBytes("platForm");
        private final byte[] softVersionByte = Bytes.toBytes("softVersion");
        private final byte[] gatherTimeByte = Bytes.toBytes("gatherTime");
        private final byte[] sourceIdByte = Bytes.toBytes("sourceId");
        private final byte[] isUninstalledByte = Bytes.toBytes("isUninstalled");
        private final byte[] isHiddenByte = Bytes.toBytes("isHidden");
        private final byte[] hashCodeByte = Bytes.toBytes("hashCode");
        private final byte[] pByte = Bytes.toBytes("p");
        //等待写入hbase的put集合
        private final StringBuilder keyPrefixBuilder = new StringBuilder(40);
        private final List<Put> putList = new ArrayList<Put>(50000);
        //hbase已有part_imei数据缓存
        private final Filter filter = new SingleColumnValueFilter(this.columnFamily, this.isUninstalledByte, CompareOp.EQUAL, Bytes.toBytes("0"));
        private final Map<String, String> softMap = new HashMap<String, String>(256, 1);
        //
        private long inputNum = 0;
        private long uniqueNum = 0;
        private long existNum = 0;
        private double putNum = 0;
        private long startTime = 0;
        private final StringBuilder mesBuilder = new StringBuilder(256);
        private double scanNum = 0;
//        private final Text newKey = new Text();
//        private final Text newValue = new Text();
        //

        /**
         * 获取前缀为rowKeyPrefix的数据
         */
        private void inquireRowKeyByPrefix() {
            ResultScanner rs = null;
            Scan scan = new Scan();
            scan.setMaxVersions();
            scan.setStartRow(Bytes.toBytes(this.rowKeyPrefix));
            scan.setStopRow(Bytes.toBytes(this.rowKeyPrefix.concat("_")));
            scan.setCacheBlocks(false);
            scan.setFilter(this.filter);
            scan.setCaching(300);
            scan.addColumn(this.columnFamily, this.softVersionByte);
            scan.addColumn(this.columnFamily, this.isUninstalledByte);
            try {
                byte[] rowKey;
                byte[] versionByte;
                String softId;
                String version;
                rs = this.hTable.getScanner(scan);
                Result result = rs.next();
                while (result != null) {
                    rowKey = result.getRow();
                    softId = Bytes.toString(rowKey);
                    softId = softId.substring(softId.lastIndexOf("_") + 1);
                    versionByte = result.getValue(this.columnFamily, this.softVersionByte);
                    version = Bytes.toString(versionByte);
                    this.softMap.put(softId, version);
                    result = rs.next();
                }
                this.scanNum++;
                this.existNum += this.softMap.size();
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                if (rs != null) {
                    rs.close();
                }
            }
        }

//        private void getRowKey(String softId) {
//            String rowKey = this.rowKeyPrefix.concat(softId);
//            Get get = new Get(Bytes.toBytes(rowKey));
//            get.setCacheBlocks(false);
//            get.setFilter(this.filter);
//            get.setMaxVersions();
//            try {
//                Result result = this.hTable.get(get);
//                result.getRow();
//                this.scanNum++;
//            } catch (IOException ex) {
//            }
//        }
        private Put createInsertPut(String[] record) {
            //softId_gatherTime_platForm_softVersion_hashCode_p
            String softId = record[0];
            String gatherTime = record[1];
            String platForm = record[2];
            String softVersion = record[3];
            String sourceId = "1";
            String isUninstalled = "0";
            String isHidden = "0";
            String hashCode = record[4];
            String p = record[5];
            String rowKey = this.rowKeyPrefix.concat(softId);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.setWriteToWAL(false);
            put.add(this.columnFamily, this.gatherTimeByte, Bytes.toBytes(gatherTime));
            put.add(this.columnFamily, this.platFormByte, Bytes.toBytes(platForm));
            put.add(this.columnFamily, this.softVersionByte, Bytes.toBytes(softVersion));
            put.add(this.columnFamily, this.sourceIdByte, Bytes.toBytes(sourceId));
            put.add(this.columnFamily, this.isHiddenByte, Bytes.toBytes(isHidden));
            put.add(this.columnFamily, this.isUninstalledByte, Bytes.toBytes(isUninstalled));
            put.add(this.columnFamily, this.hashCodeByte, Bytes.toBytes(hashCode));
            put.add(this.columnFamily, this.pByte, Bytes.toBytes(p));
            return put;
        }
        
        private Put createUpdatePut(String[] record) {
            //softId_gatherTime_platForm_softVersion_hashCode_p
            String softId = record[0];
            String gatherTime = record[1];
            String softVersion = record[3];
            String isUninstalled = "0";
            String rowKey = this.rowKeyPrefix.concat(softId);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.setWriteToWAL(false);
            put.add(this.columnFamily, this.gatherTimeByte, Bytes.toBytes(gatherTime));
            put.add(this.columnFamily, this.softVersionByte, Bytes.toBytes(softVersion));
            put.add(this.columnFamily, this.isUninstalledByte, Bytes.toBytes(isUninstalled));
            return put;
        }
        
        private Put createDeletePut(String softId) {
            //softId_gatherTime_platForm_softVersion_hashCode_p
            String isUninstalled = "1";
            String rowKey = this.rowKeyPrefix.concat(softId);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.setWriteToWAL(false);
            put.add(this.columnFamily, this.isUninstalledByte, Bytes.toBytes(isUninstalled));
            return put;
        }

        //业务处理
        private void execute() {
            //从hbase获取前缀为rowKeyPrefix的数据
            this.inquireRowKeyByPrefix();
            //比较输入数据和缓存数据
            String softId;
            String newVersion;
            String oldVersion;
            Put put;
            String[] record;
            for (String value : this.valueList) {
                softId = value.substring(0, value.indexOf("_"));
                record = value.split("_");
                newVersion = record[3];
                oldVersion = this.softMap.get(softId);
                if (oldVersion == null) {
                    //新增
                    put = this.createInsertPut(record);
                    this.putList.add(put);
                } else {
                    //已存在,比较版本修改
                    if (oldVersion.endsWith(newVersion) == false) {
                        //版本变化
                        put = this.createUpdatePut(record);
                        this.putList.add(put);
                    }
                    this.softMap.remove(softId);
                }
            }
            //如果softIdList集合size不为0,则剩下的softId则认为已经不用户删除
            Set<String> softIdSet = this.softMap.keySet();
            for (String deleteSoftId : softIdSet) {
                //够造标记逻辑删除put
                put = this.createDeletePut(deleteSoftId);
                this.putList.add(put);
            }
        }

        /**
         * reducer 初始化
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.tableName = context.getConfiguration().get(AbstractJobStart.TABLE_NAME_PARA);
            this.hTable = new HTable(context.getConfiguration(), this.tableName);
            this.startTime = System.currentTimeMillis();
            this.hTable.setAutoFlush(false);
            this.hTable.setWriteBufferSize(536870912);
        }
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyRecord = key.toString().split("_");
            String imei = keyRecord[1];
            String partHex = keyRecord[0];
            if (partHex.equals(this.lastPartHex) == false) {
                //part变化，保存已有数据
                if (this.putList.isEmpty() == false) {
                    this.hTable.put(this.putList);
                    this.hTable.flushCommits();
                    long currentTime = System.currentTimeMillis();
                    this.putNum += this.putList.size();
                    int scanSpeed = (int) (this.scanNum * 1000 / (currentTime - this.startTime));
                    int putSpeed = (int) (this.putNum * 1000 / (currentTime - this.startTime));
                    this.mesBuilder.append("part:").append(this.lastPartHex)
                            .append(" scan speed:").append(scanSpeed).append("row/s")
                            .append(" put speed:").append(putSpeed).append("row/s")
                            .append(" inputNum:").append(this.inputNum)
                            .append(" uniqueNum:").append(this.uniqueNum)
                            .append(" scanNum:").append((int) this.scanNum)
                            .append(" existNum:").append(this.existNum)
                            .append(" putNum:").append((int) this.putNum);
                    String msg = this.mesBuilder.toString();
                    this.mesBuilder.setLength(0);
                    System.out.println(msg);
//                    this.newKey.set(this.lastParHex);
//                    this.newValue.set(msg);
//                    context.write(newKey, newValue);
                    //
                    this.putList.clear();
                }
                this.lastPartHex = partHex;
            }
            //reducer端数据去重,相同的part_imei_softId,取gatherTime最大的记录 以减少向reducer传输的数据大小
            //取值
            for (Text value : values) {
                this.allValueList.add(value.toString());
            }
            this.inputNum += this.allValueList.size();
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
            this.uniqueNum += this.valueList.size();
            //清除原始数据集合
            this.allValueList.clear();
            //业务处理
            //生成当前数据的hTable的rowKey前缀
            this.keyPrefixBuilder.append(partHex).append('_').append(imei).append('_');
            this.rowKeyPrefix = keyPrefixBuilder.toString();
            this.keyPrefixBuilder.setLength(0);
            this.execute();
            //清空业务数据集合
            this.valueList.clear();
            this.softMap.clear();
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
//            最后一个partHex的数据处理
            if (this.putList.isEmpty() == false) {
                try {
                    this.hTable.put(this.putList);
                    this.hTable.flushCommits();
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
                this.putList.clear();
            }
        }
    }
}
