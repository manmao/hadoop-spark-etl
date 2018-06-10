package com.mofang.data.hadoop.etl.ratio;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.mofang.data.componet.RedisCacheUtil;
import com.mofang.data.componet.RedisTemplateFactory;
import com.mofang.data.hadoop.etl.ratio.ItemRatio.ResultRetio;

public class ItemRetioReudcer extends Reducer<Text, ItemResultWritable, Text, NullWritable> {
    
    @Override
    protected void setup(Context context){
        RedisCacheUtil.init(RedisTemplateFactory.getRedisTemplate());
    }
    
    @Override
    protected void reduce(Text key, Iterable<ItemResultWritable> values, Context context)
        throws IOException, InterruptedException {
        
        ItemRatio itemRetio = computRetio(key, values);
        
        RedisCacheUtil.set(itemRetio.getName(), itemRetio);
        
        context.write(new Text(itemRetio.getName()), NullWritable.get());
    }
    
    @Override
    protected void cleanup(Context context) {
        
    }
    
    
    /**
     * 计算结果保存内容
     * @param key
     * @param values
     * @return
     */
    private  ItemRatio computRetio(Text key, Iterable<ItemResultWritable> values) {
        
        ItemRatio itemRetio=new ItemRatio();
        
        int retioCount=0;
        String name="";
        
        Map<String, Integer> countMap=new HashMap<String,Integer>();
        //计算项目的每个结果的个数
        for (ItemResultWritable writable:values) {
            retioCount++;
            name=writable.getName();
            Integer count=countMap.get(writable.getResult());
            if(count==null) {
                countMap.put(writable.getResult(),1);
            }else {
                countMap.put(writable.getResult(),++count);
            }
        }
        
        itemRetio.setRetioCount(retioCount);
        itemRetio.setId(key.toString());
        itemRetio.setName(name);
        
        List<ResultRetio> resultRetios=new ArrayList<>();
        
        //遍历 ，计算比例
        for(Map.Entry<String, Integer> entry:countMap.entrySet()) {
            ResultRetio resultRetio=new ResultRetio();
            resultRetio.setResult(entry.getKey());
            resultRetio.setCount(entry.getValue());
            resultRetio.setRatio((float)entry.getValue()*100/(float)retioCount);
            resultRetios.add(resultRetio);
        }
        
        itemRetio.setResultRetios(resultRetios);

        return itemRetio;
    }

}
