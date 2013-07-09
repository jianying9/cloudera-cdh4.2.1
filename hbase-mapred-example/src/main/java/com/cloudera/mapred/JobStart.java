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

public class JobStart extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.out.println("start............");
        Configuration config = HBaseConfiguration.create();
//        config.set("hbase.zookeeper.quorum", "aladdin.com");
//        config.set("hbase.security.authentication", "kerberos");
//        config.set("hbase.rpc.engine", "org.apache.hadoop.hbase.ipc.SecureRpcEngine");
//        config.set("hbase.regionserver.kerberos.principal", "hbase/aladdin.com@ALADDIN.COM");
//        config.set("hbase.regionserver.keytab.file", "/etc/krb5kdc/kadm5.keytab");
//        config.set("hbase.master.kerberos.principal", "hbase/aladdin.com@ALADDIN.COM");
//        config.set("hbase.master.keytab.file", "/etc/krb5kdc/kadm5.keytab");
        int res = ToolRunner.run(config, new JobStart(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        int result;
        Job job = new Job(conf, "hdfs-hbase-example");
        job.setJarByClass(HdfsToHbaseExample.class);
        job.setMapperClass(HdfsToHbaseExample.MyMapper.class);
        job.setReducerClass(HdfsToHbaseExample.MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputFormatClass(NullOutputFormat.class);
        TableMapReduceUtil.initCredentials(job);
        result = job.waitForCompletion(true) ? 0 : 1;
        return result;
    }
}
