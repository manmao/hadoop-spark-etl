package com.mofang.data.hadoop.etl.export;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TableExportReducer extends Reducer<Text, Text, Text,Text>{
    
    @Override
    public void reduce(Text key, Iterable<Text> values,Context context)
                    throws IOException, InterruptedException {
        for(Text put: values){
            context.write(key, put);
        }
    }
    
}
