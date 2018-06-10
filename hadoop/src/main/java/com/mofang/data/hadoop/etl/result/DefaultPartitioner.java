package com.mofang.data.hadoop.etl.result;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class DefaultPartitioner extends Partitioner<Text, Put>{
    
    @Override
    public int getPartition(Text key, Put value, int numPartitions) {
        String keyStr=HashUtil.getRandomString(16)+key.toString();
        int hashCode= HashUtil.murmurHash2(keyStr.toCharArray(), keyStr.toCharArray().length);
        return Math.abs(hashCode%numPartitions);
    }
    
}