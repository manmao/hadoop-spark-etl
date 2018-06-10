package com.mofang.spark.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function0;

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

    /**
     * 將DataFrame寫入Hive
     * @param hiveContext
     */
    public  static  void insertData(HiveContext hiveContext){
        DataFrame df=mysqlDataToDF(hiveContext);
        df.registerTempTable("mysql_test");

        hiveContext.sql("set hive.exec.dynamic.partition.mode = nonstrict");
        hiveContext.sql("insert overwrite table test partition(`dt`)  select * from mysql_test");

    }


    /**
     * spark sql mysql 數據加載
     * @param hiveContext
     * @return
     */
    public  static   DataFrame mysqlDataToDF(HiveContext hiveContext){
        Properties properties = new Properties();
        properties.put("user","feigu");
        properties.put("password","feigu");
        String url = "jdbc:mysql://slave02:3306/testdb?useUnicode=true&characterEncoding=gbk&zeroDateTimeBehavior=convertToNull";
        return hiveContext.read().jdbc(url,"mysql_test",properties);
    }

    /**
     * 將原始文件加載成 DataFrame
     * @param sc
     * @param hiveContext
     * @return
     */
    public  static  DataFrame rowFileDataToDF(JavaSparkContext sc,HiveContext hiveContext){

        JavaRDD<String> lines = sc.textFile("E://test.txt");

        /** * 第一步：在RDD的基础上创建类型为Row的RDD */
        JavaRDD<Row> personsRDD = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] splited = line.split(",");
                return RowFactory.create(Integer.valueOf(splited[0]), splited[1],Integer.valueOf(splited[2]));
            }
        });

        /*** 第二步：动态构造DataFrame的元数据，一般而言，有多少列以及每列的具体类型可能来自于JSON文件，也可能来自于DB */
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        //构建StructType，用于最后DataFrame元数据的描述
        StructType structType =DataTypes.createStructType(structFields);

        /*** 第三步：基于以后的MetaData以及RDD<Row>来构造DataFrame*/
        DataFrame personsDF = hiveContext.createDataFrame(personsRDD, structType);

       /* *//** 第四步：注册成为临时表以供后续的SQL查询操作*//*
        personsDF.registerTempTable("persons");
        *//** 第五步，进行数据的多维度分析*//*
        DataFrame result = hiveContext.sql("select * from persons where age >20");
        *//**第六步：对结果进行处理，包括由DataFrame转换成为RDD<Row>，以及结构持久化*//*
        List<Row> listRow = result.javaRDD().collect();
        for(Row row : listRow){
            System.out.println(row);
        }*/
        return personsDF;
    }


    /**
     * 自定義UDF
     * @param hiveContext
     */
    public  static  void createUDF(HiveContext hiveContext){
        hiveContext.udf().register("computeLength",(String s) ->{
            return s.length();
        },DataTypes.IntegerType);
        hiveContext.sql("select word, computeLength(word) as length from bigDataTable").show();
    }

}
