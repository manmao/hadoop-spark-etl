package com.mofang.data.hadoop.etl.export;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TableExportMapper extends TableMapper<Text, Text> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value,
        Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)
        throws IOException, InterruptedException {
        
        List<Cell> cells=value.listCells();
        String valueOut="";
        for(Cell cell:cells) {
            valueOut += new String(CellUtil.cloneValue(cell))+"\t";
        }
        
        context.write(new Text(key.get()), new Text(valueOut));
        
    }
    
   
}
