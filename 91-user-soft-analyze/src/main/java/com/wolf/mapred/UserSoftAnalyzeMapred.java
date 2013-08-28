package com.wolf.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author aladdin
 */
public class UserSoftAnalyzeMapred {

    /**
     * 读取hdfs文件内容，输出固定key,value集合
     */
    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        
        private final Text newKey = new Text();
        private final IntWritable newValue = new IntWritable(1);
        private String strValue;
        private String[] record;
        private String imei;
        private String part;
        private String region;
        private String regionNext;
        private final List<String> regionList = new ArrayList<String>(512);
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //hbase默认512个region的startKey分布
            List<String> startKeyList = PartitionUtils.getPartitionStartKeyList();
            this.regionList.addAll(startKeyList);
        }

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
                this.part = PartitionUtils.getPartition(this.imei);
                this.part = this.part.concat("0000");
                for (int index = 0; index < this.regionList.size(); index++) {
                    region = this.regionList.get(index);
                    if ((index + 1) >= this.regionList.size()) {
                        //最后一个区
                        this.newKey.set(this.region);
                        break;
                    } else {
                        regionNext = this.regionList.get(index + 1);
                        if (this.part.compareTo(region) >= 0 && this.part.compareTo(regionNext) < 0) {
                            this.newKey.set(this.region);
                            break;
                        }
                    }
                }
                context.write(newKey, newValue);
            }
        }
    }

    /**
     * combiner和reducer功能一致
     */
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            //对value进行求和，然后输出
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
