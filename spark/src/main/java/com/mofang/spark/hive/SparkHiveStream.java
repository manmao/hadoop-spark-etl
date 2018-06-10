package com.mofang.spark.hive;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

public class SparkHiveStream {
    
    public static void main(String args[]) {
        
        SparkConf sparkConf = new SparkConf().setAppName("SparkHiveStream");
        
        JavaSparkContext context=new JavaSparkContext(sparkConf);
        
        HiveContext hiveContext=new HiveContext(context);

        DataFrame frame=hiveContext.sql("show tables");
        List<Row> list=frame.collectAsList();

        for(Row row:list) {
            System.out.println(row.getAs("tableName").toString());
        }
        
    }
}
