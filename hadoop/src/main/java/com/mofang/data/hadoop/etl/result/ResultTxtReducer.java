package com.mofang.data.hadoop.etl.result;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;


public class ResultTxtReducer extends TableReducer<Text, Put, ImmutableBytesWritable>{
    
    @Override
    public void reduce(Text key, Iterable<Put> values,Context context)
                    throws IOException, InterruptedException {
        // keyout   ImmutableBytesWritable
        // valueout 默认为Put
        for(Put put: values){
            context.write(null, put);
        }
    }
    
}
