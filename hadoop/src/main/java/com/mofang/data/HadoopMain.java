package com.mofang.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.mofang.data.hadoop.etl.export.TableExportDriver;
import com.mofang.data.hadoop.etl.ratio.ItemRatioDriver;
import com.mofang.data.hadoop.etl.result.ResultTxtDriver;


/**
 * 
 *
 */
public class HadoopMain 
{
    public static void main( String[] args ) throws Exception{
        
        Tool tool=null;
        
        if("resultTxt".equals(args[0])) { // etl mongodb resultTxt集合到HBase
            
            tool= new ResultTxtDriver();
            
        }else if("hbaseTableExport".equals(args[0])) { // hbase表导出
            
            tool= new TableExportDriver();
            
        }else if("itemRatio".equals(args[0])) { // hbase表导出
            
            tool= new ItemRatioDriver();
            
        }else {
            System.out.println("未指定job类型");
            return;
            
        }
        
        int res = ToolRunner.run(new Configuration(), tool, args);
        System.exit(res);
        
        
        
    }
}
