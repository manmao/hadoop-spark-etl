package com.mofang.data.hadoop.etl.export;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import com.mofang.data.HadoopMain;



/**
 * 
 * @author manmao
 * 
 * 导出hbase表内容
 * 
 * hadoop jar xxx.jar hbaseTableExport  tableName  out/put/path
 */
public class TableExportDriver extends Configured implements Tool {
    
    private static final String JAVA_HOME="/usr/local/jdk1.8.0_131";
    
    @Override
    public int run(String[] arg0) throws Exception {
        
        System.setProperty("JAVA_HOME",JAVA_HOME);
        
        Configuration conf = HBaseConfiguration.create();
        
        conf.addResource("hadoop.xml");
        conf.addResource("hbase-site.xml");
        conf.addResource("core-site.xml");
        
        Job job = Job.getInstance(conf);
        job.setJarByClass(HadoopMain.class);
        
        job.setMapperClass(TableExportMapper.class);
        job.setReducerClass(TableExportReducer.class);
        
        Scan scan = new Scan();
        scan.setCaching(2000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);   // don't set to true for MR jobs
        TableMapReduceUtil.initTableMapperJob(arg0[1],scan, TableExportMapper.class, Text.class, Text.class, job);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileSystem fs = FileSystem.get(conf); 
        fs.delete(new Path(arg0[2]), true);
        FileOutputFormat.setOutputPath(job, new Path(arg0[2]));
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setNumReduceTasks(8);
        job.waitForCompletion(true);
        
        return 0;
    }
    
}

    
