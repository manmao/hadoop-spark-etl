package com.mofang.data.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import java.io.IOException;

/**
 * 生成HFile的 导入 MR程序
 * 
 * hadoop jar xxx.jar table inputPath outputPath
 */
public class HFileGenerator {
    
    
    private static final String JAVA_HOME="/usr/local/jdk1.8.0_131";
    
    private static final String USER_NAME="hbase";
    
    private static final String  HADOOP_USER_NAME="hbase";
    
    
    public static class HFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
       
        @Override
        protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>.Context context)
            throws IOException, InterruptedException {
          //FileSplit split = (FileSplit) context.getInputSplit();
            
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split("\t");

            //行键
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(items[0].getBytes());

            // byte[] row, byte[] family, byte[] qualifier, long timestamp, byte[] value
            KeyValue beforeKv = new KeyValue(Bytes.toBytes(items[0]),
                    "info".getBytes(),
                    "before".getBytes(),
                    System.currentTimeMillis(), Bytes.toBytes(items[1]));
            context.write(rowkey, beforeKv);
            
            KeyValue afterKv = new KeyValue(Bytes.toBytes(items[0]),
                "info".getBytes(),
                "after".getBytes(),
                System.currentTimeMillis(), Bytes.toBytes(items[2]));
            context.write(rowkey, afterKv);
        }
    }


    public static void main(String[] args) throws Exception {
        
        System.setProperty("user.name",USER_NAME);
        System.setProperty("HADOOP_USER_NAME",HADOOP_USER_NAME);
        System.setProperty("JAVA_HOME",JAVA_HOME);
        
        Configuration conf = new Configuration();
        conf.addResource("hadoop.xml");
        conf.addResource("hbase-site.xml");
        conf.addResource("core-site.xml");
        
        String[] dfsArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
       
        Job job = Job.getInstance(conf);
        job.setJarByClass(HFileGenerator.class);

        job.setMapperClass(HFileMapper.class);
        job.setReducerClass(KeyValueSortReducer.class); //对reduce的KeyValue进行更改

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        //设置全局排序
        job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(dfsArgs[1]));
        
        FileSystem fs = FileSystem.get(conf); 
        fs.delete(new Path(dfsArgs[2]), true);
        FileOutputFormat.setOutputPath(job, new Path(dfsArgs[2]));

        Connection connection = ConnectionFactory.createConnection(conf);
        TableName tableNameO = TableName.valueOf(dfsArgs[0]);
        Table table=connection.getTable(tableNameO);
        Admin admin = connection.getAdmin();
        RegionLocator regionLocator=connection.getRegionLocator(tableNameO);
        
        HFileOutputFormat2.configureIncrementalLoad(job, table,regionLocator);
        
        System.out.println("生成HFile中......");
        int ret=job.waitForCompletion(true) ? 0 : 1;
        System.out.println("生成HFile成功!");
        if(ret==0) {
            System.out.println("开始导入HFile到HBase表 ==> url: "+dfsArgs[2]);
            LoadIncrementalHFiles lf = new LoadIncrementalHFiles(conf);  
            lf.doBulkLoad(new Path(dfsArgs[2]), admin,table,regionLocator);
        }
    }
}
