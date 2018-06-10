package com.mofang.data.hadoop.etl.result;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import com.mofang.data.HadoopMain;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;


/**
 * 
 * 将用户数据清洗到hbase
 * 
 * @author manmao
 * 
 */
public class ResultTxtDriver extends Configured implements Tool {
    
    @Override
    public int run(String[] arg0) throws Exception {
        
        Configuration conf = HBaseConfiguration.create();
        conf.addResource("mongodb.xml");
        
        MongoConfigUtil.setSplitSize(conf, 1024); //设置输入分片大小，单位MB
        MongoConfigUtil.setInputURI(conf,conf.get("mongo.input.uri"));
        
        //DBObject dbObject=new BasicDBObject("creationTime",new BasicDBObject("$gte",));
        //MongoConfigUtil.setQuery(conf, dbObject);
        
        Job job = Job.getInstance(conf);
        job.setJarByClass(HadoopMain.class);
        
        job.setMapperClass(ResultTxtMapper.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Put.class);
        
        job.setInputFormatClass(MongoInputFormat.class);
        
       
        // set reducer and output
        TableMapReduceUtil.initTableReducerJob(
           "resultTxt",               // output table
           ResultTxtReducer.class,    // reducer class
           job
        );
          
        job.setNumReduceTasks(16);
        
        job.setPartitionerClass(DefaultPartitioner.class);
        
        job.waitForCompletion(true);
        
        return 0;
    }
}

    
