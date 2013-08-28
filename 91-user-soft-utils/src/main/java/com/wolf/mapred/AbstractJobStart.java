package com.wolf.mapred;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.Tool;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author aladdin
 */
public abstract class AbstractJobStart extends Configured implements Tool {

    public static final String TABLE_NAME_PARA = "-Dmapred.tableName";
    public static final String TABLE_REGION_PART = "-Dmapred.tableRegionPart";
    private final String parameterPath = "parameter.json";
    private final Map<String, ParameterEntity> parameterMap = new HashMap<String, ParameterEntity>(16, 1);

    public AbstractJobStart() {
        //读取参数文件
        String path = AbstractJobStart.class.getClassLoader().getResource(this.parameterPath).getPath();
        if (path.isEmpty()) {
            System.out.println("--not find parameter.json.");
        } else {
            System.out.println("read properties path:" + path);
            JsonNode rootNode = null;
            File file = new File(path);
            ObjectMapper mapper = new ObjectMapper();
            try {
                rootNode = mapper.readValue(file, JsonNode.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (rootNode != null) {
                //读数据
                Map.Entry<String, JsonNode> entry;
                String name;
                String value;
                String describe;
                Iterator<Map.Entry<String, JsonNode>> iterator = rootNode.getFields();
                JsonNode jsonNode;
                ParameterEntity parameterEntity;
                while (iterator.hasNext()) {
                    entry = iterator.next();
                    name = entry.getKey();
                    jsonNode = entry.getValue().get("value");
                    if (jsonNode == null) {
                        value = "";
                    } else {
                        value = jsonNode.getTextValue();
                    }
                    jsonNode = entry.getValue().get("describe");
                    if (jsonNode == null) {
                        describe = "";
                    } else {
                        describe = jsonNode.getTextValue();
                    }
                    parameterEntity = new ParameterEntity(name, describe);
                    parameterEntity.setValue(value);
                    this.parameterMap.put(name, parameterEntity);
                }
            }
        }
    }

    public final String getParameter(String name) {
        return this.parameterMap.get(name).getValue();
    }

    /**
     * 实例化job
     *
     * @return
     * @throws Exception
     */
    public abstract Job createJob() throws Exception;

    /**
     * 获取必要参数
     *
     * @return
     */
    public abstract String[] getValidateParameter();

    @Override
    public final int run(String[] args) throws Exception {
        int result = 0;//return
        if (args.length > 0 && (args[0].equals("-h") || args[0].equals("--help"))) {
            //帮助说明
            this.parameterMap.entrySet();
            Set<Entry<String, ParameterEntity>> entrySet = this.parameterMap.entrySet();
            for (Entry<String, ParameterEntity> entry : entrySet) {
                System.out.println("--name:" + entry.getKey() + " value:" + entry.getValue().getValue());
                System.out.println("----describe:" + entry.getValue().getDescribe());
            }
        } else {
            //处理输入参数
            String text;
            String[] para;
            ParameterEntity parameterEntity;
            for (int index = 0; index < args.length; index++) {
                text = args[index];
                para = text.split("=");
                if (para.length == 2) {
                    //符合要求的参数格式
                    parameterEntity = this.parameterMap.get(para[0]);
                    if (parameterEntity != null && para[1].isEmpty() == false) {
                        parameterEntity.setValue(para[1]);
                    }
                }
            }
            //验证参数
            String[] paras = this.getValidateParameter();
            String name;
            String value;
            for (int index = 0; index < paras.length; index++) {
                name = paras[index];
                value = this.getParameter(name);
                if (value == null || value.isEmpty()) {
                    String message = "miss parameter:" + name;
                    throw new RuntimeException(message);
                } else {
                    System.out.println("parameter info----name:" + name + "  value:" + value);
                }
            }
            //创建job，并执行
            Job job = this.createJob();
            result = job.waitForCompletion(true) ? 0 : 1;
        }
        return result;
    }

    protected final void initJobByHTablePartition(Job job, String tableName, final int loadFactor, final int distance) throws IOException {
        HTable hTable = new HTable(this.getConf(), tableName);
        NavigableMap<HRegionInfo, ServerName> regionMap = hTable.getRegionLocations();
        Set<Map.Entry<HRegionInfo, ServerName>> entrySet = regionMap.entrySet();
        String startKey;
        String endKey;
        String hostName;
        HRegionInfo hRegionInfo;
        ServerName serverName;
        //获取region server的数量
        LinkedList<String> reginLinkedList;
        final List<String> regionHostList = new ArrayList<String>(32);
        final Map<String, LinkedList> regionHostMap = new HashMap<String, LinkedList>(32, 1);
        System.out.println("--region server host");
        for (Map.Entry<HRegionInfo, ServerName> entry : entrySet) {
            serverName = entry.getValue();
            hostName = serverName.getHostname();
            if (regionHostMap.containsKey(hostName) == false) {
                reginLinkedList = new LinkedList<String>();
                System.out.println("----region server:" + hostName);
                regionHostList.add(hostName);
                regionHostMap.put(hostName, reginLinkedList);
            }
        }
        System.out.println("--region server host--");
        System.out.println("--region partition");
        //记录每个region的startKey，并根据region所在的region server分类
        final List<String> startKeyList = new ArrayList<String>(512);
        for (Map.Entry<HRegionInfo, ServerName> entry : entrySet) {
            serverName = entry.getValue();
            hostName = serverName.getHostname();
            reginLinkedList = regionHostMap.get(hostName);
            hRegionInfo = entry.getKey();
            startKey = Bytes.toString(hRegionInfo.getStartKey());
            if (startKey.isEmpty()) {
                startKey = "00000000";
            }
            reginLinkedList.offer(startKey);
            startKeyList.add(startKey);
            endKey = Bytes.toString(hRegionInfo.getEndKey());
            System.out.println("---hostName:" + hostName + " startKey:" + startKey + " endKey:" + endKey);
        }
        System.out.println("--region partition--");
        System.out.println("--region part");
        //为每个region分配区号
        StringBuilder partInfoBuilder = new StringBuilder(10240);
        final Map<String, Integer> partMap = new HashMap<String, Integer>(startKeyList.size(), 1);
        int partIndex = 0;
        int part;
        boolean unFinished = true;
        while (unFinished) {
            unFinished = false;
            for (String regionHost : regionHostList) {
                reginLinkedList = regionHostMap.get(regionHost);
                if (reginLinkedList.isEmpty() == false) {
                    part = partIndex;
                    //取值
                    for (int index = 0; index < loadFactor; index++) {
                        startKey = reginLinkedList.poll();
                        if (startKey != null) {
                            partMap.put(startKey, part);
                        }
                    }
                    partIndex++;
                    unFinished = true;
                    //偏移
                    for (int index = 0; index < distance; index++) {
                        startKey = reginLinkedList.poll();
                        if (startKey == null) {
                            break;
                        } else {
                            reginLinkedList.offer(startKey);
                        }
                    }
                }
            }
        }
        //保存region分区信息
        for (String sKey : startKeyList) {
            part = partMap.get(sKey);
            partInfoBuilder.append(sKey).append('_').append(Integer.toString(part)).append("\t");
            System.out.println("-----startKey:" + sKey + " part:" + part);
        }
        partInfoBuilder.setLength(partInfoBuilder.length() - "\t".length());
        System.out.println("----------------region part----------------");
        //获取numReduceTask的数量
        int numReduceTask = partIndex;
        System.out.println("------------NumReduceTasks:" + numReduceTask);
        job.setNumReduceTasks(numReduceTask);
        job.getConfiguration().set(AbstractJobStart.TABLE_REGION_PART, partInfoBuilder.toString());
    }

    protected final void initJobByHTablePartition(Job job, String tableName) throws IOException {
        this.initJobByHTablePartition(job, tableName, 1, 0);
    }
    
    /**
     * 根据htable的region分布对数据进行分区
     */
    public static class HTablePartitioner extends Partitioner<Text, Text> implements Configurable {

        /*
         * region startKe集合，00000000～fff0000顺序存储 
         */
        private final List<String> regionList = new ArrayList<String>(512);
        /*
         * region 和 partition对应关系
         */
        private final Map<String, Integer> regionPartMap = new HashMap<String, Integer>(512, 1);
        private Configuration conf;
        
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            int result;
            String keyStr = key.toString();
            String part = keyStr.substring(0, keyStr.indexOf("_"));
            part = part.concat("0000");
            String region = "00000000";
            String regionNext;
            for (int index = 0; index < this.regionList.size(); index++) {
                region = this.regionList.get(index);
                if ((index + 1) >= this.regionList.size()) {
                    //最后一个region
                    break;
                } else {
                    //非最后一个region
                    regionNext = this.regionList.get(index + 1);
                    if (part.compareTo(region) >= 0 && part.compareTo(regionNext) < 0) {
                        //定位当前part所属的region
                        break;
                    }
                }
            }
            result = this.regionPartMap.get(region);
            return result;
        }
        
        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
            //获取conf中hTable的region分布信息
            String regionPartString = this.conf.get(AbstractJobStart.TABLE_REGION_PART);
            String[] regionParts = regionPartString.split("\t");
            String[] region;
            String startKey;
            int part;
            for (String regionPart : regionParts) {
                region = regionPart.split("_");
                startKey = region[0];
                part = Integer.parseInt(region[1]);
                this.regionList.add(startKey);
                this.regionPartMap.put(startKey, part);
                System.out.println("------------startKey:" + startKey + " part:" + part);
            }
        }
        
        @Override
        public Configuration getConf() {
            return this.conf;
        }
    }
}
