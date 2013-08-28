package com.wolf.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author aladdin
 */
public class JobStart extends AbstractJobStart {

    /**
     *
     * @param args 0:input path; 1:numReducerTask; 2:htable name
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        int res = ToolRunner.run(config, new JobStart(), args);
        System.exit(res);
    }

    @Override
    public Job createJob() throws Exception {
        Configuration conf = this.getConf();
        //获取htable的region,region server信息
        final String tableName = this.getParameter("tableName");
        final String inputPath = this.getParameter("inputPath");
        final int loadFactory = Integer.parseInt(this.getParameter("loadFactor"));
        //初始化job
        Job job = new Job(conf, "91-user-soft-load");
        job.setJarByClass(UserSoftLoadMapred.class);
        job.setMapperClass(UserSoftLoadMapred.MyMapper.class);
        job.setPartitionerClass(AbstractJobStart.HTablePartitioner.class);
        job.setReducerClass(UserSoftLoadMapred.MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        job.setOutputFormatClass(NullOutputFormat.class);
        job.getConfiguration().set(AbstractJobStart.TABLE_NAME_PARA, tableName);
        //根据htable信息自动计算reducer数量和数据分区
        this.initJobByHTablePartition(job, tableName, loadFactory, 0);
        TableMapReduceUtil.initCredentials(job);
        return job;
    }

    @Override
    public String[] getValidateParameter() {
        String[] paras = {"tableName", "inputPath", "loadFactor"};
        return paras;
    }
}
