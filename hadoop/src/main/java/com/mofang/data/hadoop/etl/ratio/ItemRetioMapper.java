package com.mofang.data.hadoop.etl.ratio;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;

public class ItemRetioMapper extends Mapper<Object, BSONObject, Text, ItemResultWritable> {
    
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
        
    }
    
    @Override
    protected void map(Object key, BSONObject value, Context context)
        throws IOException, InterruptedException {
        
        BSONObject resultDetail = (BSONObject)value.get("resultDetail");
        
        // 疾病
        BSONObject diseaseResult = (BSONObject)resultDetail.get("diseaseResult");
        if(diseaseResult != null) {
            BSONObject highDiseaseResultDetails = (BSONObject)diseaseResult.get("highDiseaseResultDetails");
            if(highDiseaseResultDetails!=null) {
                BasicBSONList highDiseaseResultDetailsList = new BasicBSONList();
                highDiseaseResultDetailsList.putAll(highDiseaseResultDetails);
                reduceDiseaseResult(highDiseaseResultDetailsList,context);
            }
            
            BSONObject avgDiseaseResultDetails = (BSONObject)diseaseResult.get("avgDiseaseResultDetails");
            if(avgDiseaseResultDetails!=null) {
                BasicBSONList avgDiseaseResultDetailsList = new BasicBSONList();
                avgDiseaseResultDetailsList.putAll(avgDiseaseResultDetails);
                reduceDiseaseResult(avgDiseaseResultDetailsList,context);
            }
            
            BSONObject lowDiseaseResultDetails = (BSONObject)diseaseResult.get("lowDiseaseResultDetails");
            if(lowDiseaseResultDetails!=null) {
                BasicBSONList lowDiseaseResultDetailsList = new BasicBSONList();
                lowDiseaseResultDetailsList.putAll(lowDiseaseResultDetails);
                reduceDiseaseResult(lowDiseaseResultDetailsList,context);
            }
        }
        
        //体质
        BSONObject traitResult = (BSONObject)resultDetail.get("traitResult");
        if (traitResult != null) {
            BSONObject traitResultDetails = (BSONObject)traitResult.get("traitResultDetails");
            reduceResult(traitResultDetails,context);
        }
        
        //药物
        BSONObject drugResult = (BSONObject)resultDetail.get("drugResult");
        if (drugResult != null) {
            BSONObject drugResultDetails = (BSONObject)drugResult.get("drugResultDetails");
            reduceResult(drugResultDetails,context);
        }
        
        //罕见病
        BSONObject inheritedResult = (BSONObject)resultDetail.get("inheritedResult");
        if (inheritedResult != null) {
            BSONObject inheritedResultDetails = (BSONObject)inheritedResult.get("inheritedResultDetails");
            reduceResult(inheritedResultDetails,context);
        }
        
        //营养
        BSONObject nutritionResult = (BSONObject)resultDetail.get("nutritionResult");
        if (nutritionResult != null) {
            BSONObject nutritionResultDetails = (BSONObject)nutritionResult.get("nutritionResultDetails");
            reduceResult(nutritionResultDetails,context);
        }
        
        //运动
        BSONObject fitnessResult = (BSONObject)resultDetail.get("fitnessResult");
        if(fitnessResult != null) {
            BSONObject fitnessCategoryResults = (BSONObject)fitnessResult.get("fitnessCategoryResults");
            if(fitnessCategoryResults!=null) {
                BasicBSONList fitnessCategoryResultsList = new BasicBSONList();
                fitnessCategoryResultsList.putAll(fitnessCategoryResults);
                reduceResult(fitnessCategoryResultsList,context);
                
                for (Object object : fitnessCategoryResultsList) {
                    BSONObject itemBsonObject = (BSONObject)object;
                    BSONObject fitnessResultDetails=(BSONObject)itemBsonObject.get("fitnessResultDetails");
                    if(fitnessResultDetails!=null) {
                        BasicBSONList fitnessResultDetailsList = new BasicBSONList();
                        fitnessResultDetailsList.putAll(fitnessResultDetails);
                        reduceResult(fitnessResultDetailsList,context);
                    }
                }
            }
        }
        
    }
    
    
    public void reduceResult(BSONObject resultDetails, Context context)
        throws IOException, InterruptedException {
        
        if (resultDetails != null) {
            BasicBSONList list = new BasicBSONList();
            list.putAll(resultDetails);
            
            for (Object object : list) {
                BSONObject itemBsonObject = (BSONObject)object;
                
                //结果为空
                if(itemBsonObject.get("result")==null) 
                    continue;
                
                String id = itemBsonObject.get("_id").toString();
                String name = itemBsonObject.get("name").toString();
                String result = itemBsonObject.get("result").toString();
                if (StringUtils.isNotBlank(id) && StringUtils.isNotBlank(name) && StringUtils.isNotBlank(result)) {
                    ItemResultWritable writable = new ItemResultWritable(id, name, result);
                    context.write(new Text(id), writable);
                }
            }
        }
    }
    
    /**
     * 疾病项目结果统计
     * @param resultDetails
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduceDiseaseResult(BSONObject resultDetails, Context context) 
        throws IOException, InterruptedException {
        
        if (resultDetails != null) {
            BasicBSONList list = new BasicBSONList();
            list.putAll(resultDetails);
            
            for (Object object : list) {
                BSONObject itemBsonObject = (BSONObject)object;
                String id = itemBsonObject.get("_id").toString();
                String name = itemBsonObject.get("name").toString();
                
                String result="";
                if(itemBsonObject.get("mutationAdvice")!=null) {
                    result += itemBsonObject.get("mutationAdvice").toString();
                }
                
                if(itemBsonObject.get("resultAdvice")!=null) {
                    result += ","+itemBsonObject.get("resultAdvice").toString();
                }
                
                if(StringUtils.isNotBlank(id) && StringUtils.isNotBlank(name) && StringUtils.isNotBlank(result)) {
                    ItemResultWritable writable = new ItemResultWritable(id, name, result);
                    context.write(new Text(id), writable);
                }
            }
        }
    }
    
}
