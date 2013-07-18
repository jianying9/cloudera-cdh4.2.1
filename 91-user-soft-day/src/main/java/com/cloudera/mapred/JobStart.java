package com.cloudera.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
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
    
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        int res = ToolRunner.run(config, new JobStart(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        int result;
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
        job.setNumReduceTasks(Integer.parseInt(args[1]));
        job.getConfiguration().set(UserSoftDayMapred.TABLE_NAME_PARA, args[2]);
        TableMapReduceUtil.initCredentials(job);
        result = job.waitForCompletion(true) ? 0 : 1;
        return result;
    }
}