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
package com.mofang.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.google.common.collect.ImmutableMap;
import com.mofang.spark.vo.RemoveLog;

import scala.Tuple2;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Produces a count of events received from Flume.
 *
 * This should be used in conjunction with an AvroSink in Flume. It will start an Avro server on at the request
 * host:port address and listen for requests. Your Flume AvroSink should be pointed to this address.
 *
 * Usage: JavaFlumeEventCount <host> <port> <host> is the host the Flume receiver will be started on - a receiver
 * creates a server and listens for flume events. <port> is the port the Flume receiver will listen on.
 *
 * To run this example: `$ bin/run-example org.apache.spark.examples.streaming.JavaFlumeEventCount <host> <port>`
 *
 * 提交spar程序，poll的flume的ip和port 
 * 
 * spark-submit --master yarn-cluster xxxx.jar 192.168.30.130 10909
 *
 * 启动flume ./bin/flume-ng agent -f conf/flume-lxw-conf.properties -n agent_lxw -c conf
 * -Dflume.root.logger=DEBUG,ERROR,console
 *
 */
public final class RemoveLogFlumeEventCount {
    
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
            .setAppName("JavaFlumeEventCount")
            .set("es.index.auto.create", "true")
            .set("es.write.operation", "upsert")
            .set("es.nodes", "192.168.30.130")
            .set("es.port","9200");
        
        // 设置间隔时间
        Duration batchInterval = new Duration(1000*60*3);// 3分钟
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);
        
        // 广播变量
        //Broadcast<String> bc = ssc.sparkContext().broadcast(mongoOutputUri);
        
        // 设置checkpint
        ssc.checkpoint("/user/hdfs/data/spark/");
        
        // 拉取flume的数据
        JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createPollingStream(ssc, host, port);
        
        // 解析flume的日志
        JavaPairDStream<String, String> mapPair =
            flumeStream.mapToPair(new PairFunction<SparkFlumeEvent, String, String>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Tuple2<String, String> call(SparkFlumeEvent sparkFlumeEvent)
                    throws Exception {
                    String body = decode(sparkFlumeEvent.event().getBody());
                    if (body.split("\t").length >= 7) {
                        String barcode = body.split("\t")[1];
                        String itemName = body.split("\t")[5];
                        return new Tuple2<>(barcode + "&" + itemName, itemName);
                    } else {
                        return new Tuple2<>("", "");
                    }
                }
            });
        
        // 去重
        JavaPairDStream<String, String> uniq = mapPair.reduceByKey(new Function2<String, String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public String call(String s, String s2) throws Exception {
                return s;
            }
        });
        
        // 获取barcode生成 【 barcode - 删除项目 】
        JavaPairDStream<String, String> finalStream =
            uniq.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2)
                    throws Exception {
                    String barcode = stringStringTuple2._1().split("&")[0];
                    return new Tuple2<>(barcode, stringStringTuple2._2());
                }
            });
        
        // group by barcode
        JavaPairDStream<String, Iterable<String>> barocdeGroup = finalStream.groupByKey();
        
        /**
         * 转化成item
         */
        JavaDStream<RemoveLog> compact =
            barocdeGroup.map(new Function<Tuple2<String, Iterable<String>>, RemoveLog>() {
                private static final long serialVersionUID = 1L;
                @Override
                public RemoveLog call(Tuple2<String, Iterable<String>> stringIterableTuple2)
                    throws Exception {
                    
                    RemoveLog removeItemCount = new RemoveLog();
                    
                    removeItemCount.setBarcode(stringIterableTuple2._1());
                    
                    List<String> list = new ArrayList<>();
                    for (String item : stringIterableTuple2._2()) {
                        list.add(item);
                    }
                    removeItemCount.setItems(list);
                    removeItemCount.setCount(list.size());
                    removeItemCount.setDate(new Date());
                    return removeItemCount;
                }        
            });
        
        /**
         * 保存日志到ElasticSearch
         */
        compact.foreachRDD(rdd -> {
            JavaEsSpark.saveToEs(rdd, "report-log/log",ImmutableMap.of("es.mapping.id", "barcode"));
        });
        
        ssc.start();
        ssc.awaitTermination();
        ssc.close();
    }
    
    public static String decode(ByteBuffer bb) {
        Charset charset = Charset.forName("utf-8");
        return charset.decode(bb).toString();
    }
}