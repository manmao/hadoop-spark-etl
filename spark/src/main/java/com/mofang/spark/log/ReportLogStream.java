/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mofang.spark.log;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableMap;
import com.mofang.spark.vo.ReportLogItem;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;

/**
 *  动态读取Flume数据，保存到ES
 *  提交spar程序，poll的flume的ip和port 
 *  
 *  spark-submit --master yarn-cluster xxxx.jar 192.168.30.130 10909
 *  
 */
public final class ReportLogStream {
    
    public static void main(String[] args) throws Exception {
        
        System.setProperty("user.name","hdfs");
        System.setProperty("HADOOP_USER_NAME","hdfs");
        System.setProperty("JAVA_HOME","/usr/lib/jvm/jdk1.8.0_121");
        
        if (args.length != 2) {
            System.err.println("Usage: FlumeEventCount <host> <port>");
            System.exit(1);
        }
        
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        
        // 配置参数
        SparkConf sparkConf = new SparkConf()
            .setAppName("ReportLogStream")
            .set("spark.streaming.stopGracefullyOnShutdown","true")  //spark优雅退出
            .set("es.index.auto.create", "true")
            .set("es.write.operation", "upsert")
            .set("es.nodes", "192.168.30.130")
            .set("es.port","9200");
        
        // 设置间隔时间
        Duration batchInterval = new Duration(1000*20);//20秒钟
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);
        
        // 设置checkpint
        ssc.checkpoint("/user/hdfs/data/spark/reportLogStream/");
        
        // 拉取flume的数据
        JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createPollingStream(ssc, host, port);
        
        JavaDStream<ReportLogItem> objectRDD=flumeStream.map(new Function<SparkFlumeEvent,ReportLogItem>() {
            private static final long serialVersionUID = 1L;
            @Override
            public ReportLogItem call(SparkFlumeEvent v1)throws Exception {
                String body = decode(v1.event().getBody());
                String items[]=body.split("\t");
                ReportLogItem item=new ReportLogItem();
                if(items.length==13) {  // 遗传风险日志
                    item=convertItemCountReportLogItem(items);
                }else if(items.length == 8) { // 删除项目
                    item=convertRemoveReportLogItem(items);
                }else if(items.length == 9) { // 罕见病
                    item=convertInheritReportLogItem(items);
                }
                
                if(StringUtils.isNotBlank(item.getUniqId())) {
                    item.setUniqId("123456789abcdef");
                }
                
                return item;
            }
        });
        
        
        /**
         * 保存日志到ElasticSearch
         */
        objectRDD.foreachRDD(rdd -> {
            JavaEsSpark.saveToEs(rdd, "report-log/log",ImmutableMap.of("es.mapping.id", "uniqId"));
        });
        
        ssc.start();
        ssc.awaitTermination();
        ssc.close();
    }
    
    public static String decode(ByteBuffer bb) {
        Charset charset = Charset.forName("utf-8");
        return charset.decode(bb).toString();
    }
    
    public static ReportLogItem convertItemCountReportLogItem(String items[]) {
        ReportLogItem item=new ReportLogItem();
        item.setBarcode(items[0]);
        item.setType(LogType.ITEM_COUNT.getValue());
        item.setContent(JSON.toJSONString(items));
        item.setDate(new Date());
        item.setUniqId(item.getBarcode()+item.getType());
        return item;
    }
    
    public static ReportLogItem convertRemoveReportLogItem(String items[]) {
        ReportLogItem item=new ReportLogItem();
        item.setBarcode(items[1]);
        item.setItemId(items[4]);
        item.setType(LogType.REMOVE.getValue());
        item.setContent(JSON.toJSONString(items));
        item.setDate(new Date());
        item.setUniqId(item.getBarcode()+item.getType()+item.getItemId());
        return item;
    }
    
    public static ReportLogItem convertInheritReportLogItem(String items[]) {
        ReportLogItem item=new ReportLogItem();
        item.setBarcode(items[1]);
        item.setItemId(items[3]);
        item.setType(LogType.INHERIT.getValue());
        item.setContent(JSON.toJSONString(items));
        item.setDate(new Date());
        item.setUniqId(item.getBarcode()+item.getType()+item.getItemId());
        return item;
    }
        
    
}