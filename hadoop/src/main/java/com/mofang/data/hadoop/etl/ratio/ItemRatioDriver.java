package com.mofang.data.hadoop.etl.ratio;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import com.mofang.data.HadoopMain;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;


/*
 * 统计项目百分比
 * 
 * 数据源 130 resultTxt 数据库,统计每个项目所占比例
 * 
 * 结果保存到redis
 * 
 */
public class ItemRatioDriver extends Configured implements Tool {
   
    
    @Override
    public int run(String[] arg0) throws Exception {
        
        Configuration conf = new Configuration();
        
        //conf.addResource("hadoop.xml");
        conf.addResource("mongodb.xml");
        
        MongoConfigUtil.setInputURI(conf,conf.get("mongo.input.uri"));
        MongoConfigUtil.setOutputURI(conf,conf.get("mongo.output.uri"));
        MongoConfigUtil.setSplitSize(conf, 1024); //设置输入分片大小，单位MB
        
        Job job = Job.getInstance(conf);
        job.setJarByClass(HadoopMain.class);
        job.setMapperClass(ItemRetioMapper.class);
        job.setReducerClass(ItemRetioReudcer.class);
        job.setNumReduceTasks(8);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ItemResultWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setInputFormatClass(MongoInputFormat.class);
        job.setOutputFormatClass(MongoOutputFormat.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
    
    
   
}

    
