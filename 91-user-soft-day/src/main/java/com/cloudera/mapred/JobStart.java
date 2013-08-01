package com.cloudera.mapred;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author aladdin
 */
public class JobStart extends Configured implements Tool{
    
    /**
     * args[0]:hdfs input path
     * args[1]:hTable name
     */
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        int res = ToolRunner.run(config, new JobStart(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        int result;
        //获取htable的region,region server信息
        String tableName = args[1];
        HTable hTable = new HTable(conf, tableName);
        NavigableMap<HRegionInfo, ServerName> regionMap = hTable.getRegionLocations();
        Set<Map.Entry<HRegionInfo, ServerName>> entrySet = regionMap.entrySet();
        String startKey;
        String endKey;
        String hostName;
        HRegionInfo hRegionInfo;
        ServerName serverName;
        Map<String, Integer> hostNameMap = new HashMap<String, Integer>(32, 1);
        int partNum = 0;
        int part;
        List<String> startKeyList = new ArrayList<String>(512);
        Map<String, Integer> regionHostMap = new HashMap<String, Integer>(512, 1);
        System.out.println("----------------region host----------------");
        for (Map.Entry<HRegionInfo, ServerName> entry : entrySet) {
            serverName = entry.getValue();
            hostName = serverName.getHostname();
            if(hostNameMap.containsKey(hostName) == false) {
                hostNameMap.put(hostName, partNum);
                part = partNum;
                partNum++;
                System.out.println("------hostName:" + hostName + " part:" + part);
            }
        }
        System.out.println("----------------region host----------------");
        System.out.println("----------------region----------------");
        for (Map.Entry<HRegionInfo, ServerName> entry : entrySet) {
            serverName = entry.getValue();
            hostName = serverName.getHostname();
            part = hostNameMap.get(hostName);
            hRegionInfo = entry.getKey();
            startKey = Bytes.toString(hRegionInfo.getStartKey());
            if(startKey.isEmpty()) {
                startKey = "00000000";
            }
            endKey = Bytes.toString(hRegionInfo.getEndKey());
            startKeyList.add(startKey);
            regionHostMap.put(startKey, part);
            System.out.println("-------------------hostName:" + hostName + " startKey:" + startKey + " endKey:" + endKey);
        }
        System.out.println("----------------region----------------");
        System.out.println("----------------region part----------------");
        StringBuilder partInfoBuilder = new StringBuilder(10240);
        for (String sKey : startKeyList) {
            part = regionHostMap.get(sKey);
            partInfoBuilder.append(sKey).append('_').append(Integer.toString(part)).append("\t");
            System.out.println("-----startKey:" + sKey + " part:" + part);
        }
        partInfoBuilder.setLength(partInfoBuilder.length() - "\t".length());
        System.out.println("----------------region part----------------");
        //获取numReduceTask的数量
        int numReduceTask = hostNameMap.size();
        System.out.println("------------NumReduceTasks:" + numReduceTask);
        //初始化job
        Job job = new Job(conf, "91-user-soft-day");
        job.setJarByClass(UserSoftDayMapred.class);
        job.setMapperClass(UserSoftDayMapred.MyMapper.class);
        job.setCombinerClass(UserSoftDayMapred.MyCombiner.class);
        job.setPartitionerClass(UserSoftDayMapred.MyPartitioner.class);
        job.setReducerClass(UserSoftDayMapred.MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputFormatClass(NullOutputFormat.class);
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setNumReduceTasks(numReduceTask);
        job.getConfiguration().set(UserSoftDayMapred.TABLE_NAME_PARA, tableName);
        job.getConfiguration().set(UserSoftDayMapred.TABLE_REGION_PART, partInfoBuilder.toString());
        TableMapReduceUtil.initCredentials(job);
        result = job.waitForCompletion(true) ? 0 : 1;
        job.getWorkingDirectory();
        return result;
    }
}
