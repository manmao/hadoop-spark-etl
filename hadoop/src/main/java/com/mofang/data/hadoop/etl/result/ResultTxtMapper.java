package com.mofang.data.hadoop.etl.result;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;
import org.bson.types.BasicBSONList;

/**
 * 查询检测结果中  氯吡格雷-谨慎使用的barcode
 *         barcode-项目-结果-位点-基因型
 * @author manmao
 *
 */
public class ResultTxtMapper extends Mapper<Object, BSONObject, Text, Put> {
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        
    }
    
    @Override
    protected void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
        
        BSONObject resultDetail = (BSONObject)value.get("resultDetail");
        String barcode=value.get("barCode").toString();
        String productId=value.get("productId").toString();
        
        /**
         * 疾病
         */
        BSONObject diseaseResult = (BSONObject)resultDetail.get("diseaseResult");
        if(diseaseResult != null) {
            BSONObject highDiseaseResultDetails = (BSONObject)diseaseResult.get("highDiseaseResultDetails");
            if(highDiseaseResultDetails!=null) {
                BasicBSONList highDiseaseResultDetailsList = new BasicBSONList();
                highDiseaseResultDetailsList.putAll(highDiseaseResultDetails);
                reduceDiseaseResult(context,highDiseaseResultDetailsList,barcode,productId);
            }
            
            BSONObject avgDiseaseResultDetails = (BSONObject)diseaseResult.get("avgDiseaseResultDetails");
            if(avgDiseaseResultDetails!=null) {
                BasicBSONList avgDiseaseResultDetailsList = new BasicBSONList();
                avgDiseaseResultDetailsList.putAll(avgDiseaseResultDetails);
                reduceDiseaseResult(context,avgDiseaseResultDetailsList,barcode,productId);
            }
            
            BSONObject lowDiseaseResultDetails = (BSONObject)diseaseResult.get("lowDiseaseResultDetails");
            if(lowDiseaseResultDetails!=null) {
                BasicBSONList lowDiseaseResultDetailsList = new BasicBSONList();
                lowDiseaseResultDetailsList.putAll(lowDiseaseResultDetails);
                reduceDiseaseResult(context,lowDiseaseResultDetailsList,barcode,productId);
            }
        }
        
        
        /**
         * 药物
         */
        BSONObject drugResult = (BSONObject)resultDetail.get("drugResult");
        if(drugResult != null) {
            BSONObject drugResultDetails = (BSONObject)drugResult.get("drugResultDetails");
            if(drugResultDetails!=null) {
                BasicBSONList drugResultDetailsList = new BasicBSONList();
                drugResultDetailsList.putAll(drugResultDetails);
                reduceResult(context,drugResultDetailsList,barcode,productId);
            }
        }
        
        
        /**
         * 体质特征
         */
        BSONObject traitResult = (BSONObject)resultDetail.get("traitResult");
        if(traitResult != null) {
            BSONObject traitResultDetails = (BSONObject)traitResult.get("traitResultDetails");
            if(traitResultDetails!=null) {
                BasicBSONList traitResultDetailsList = new BasicBSONList();
                traitResultDetailsList.putAll(traitResultDetails);
                reduceResult(context,traitResultDetailsList,barcode,productId);
            }
        }
        
        /**
         * 罕见病
         */
        BSONObject inheritedResult = (BSONObject)resultDetail.get("inheritedResult");
        if(inheritedResult != null) {
            BSONObject inheritedResultDetails = (BSONObject)inheritedResult.get("inheritedResultDetails");
            if(inheritedResultDetails!=null) {
                BasicBSONList inheritedResultDetailsList = new BasicBSONList();
                inheritedResultDetailsList.putAll(inheritedResultDetails);
                reduceResult(context,inheritedResultDetailsList,barcode,productId);
            }
        }
        
        /**
         * 营养
         */
        BSONObject nutritionResult = (BSONObject)resultDetail.get("nutritionResult");
        if(nutritionResult != null) {
            BSONObject nutritionResultDetails = (BSONObject)nutritionResult.get("nutritionResultDetails");
            if(nutritionResultDetails!=null) {
                BasicBSONList nutritionResultDetailsList = new BasicBSONList();
                nutritionResultDetailsList.putAll(nutritionResultDetails);
                reduceResult(context,nutritionResultDetailsList,barcode,productId);
            }
        }
        
        /**
         * 运动
         */
        BSONObject fitnessResult = (BSONObject)resultDetail.get("fitnessResult");
        if(fitnessResult != null) {
            BSONObject fitnessCategoryResults = (BSONObject)fitnessResult.get("fitnessCategoryResults");
            if(fitnessCategoryResults!=null) {
                BasicBSONList fitnessCategoryResultsList = new BasicBSONList();
                fitnessCategoryResultsList.putAll(fitnessCategoryResults);
                reduceResult(context,fitnessCategoryResultsList,barcode,productId);
                
                for (Object object : fitnessCategoryResultsList) {
                    BSONObject itemBsonObject = (BSONObject)object;
                    BSONObject fitnessResultDetails=(BSONObject)itemBsonObject.get("fitnessResultDetails");
                    if(fitnessResultDetails!=null) {
                        BasicBSONList fitnessResultDetailsList = new BasicBSONList();
                        fitnessResultDetailsList.putAll(fitnessResultDetails);
                        reduceResult(context,fitnessResultDetailsList,barcode,productId);
                    }
                }
            }
        }
    }
    
    
    private void reduceResult(Context context,BasicBSONList list,String barcode,String productId) throws IOException, InterruptedException {
        
        for (Object object : list) {
            BSONObject itemBsonObject = (BSONObject)object;
            String id = Optional.ofNullable(itemBsonObject.get("_id")).map(val->val.toString()).orElse("");
            String name = Optional.ofNullable(itemBsonObject.get("name")).map(val->val.toString()).orElse("");
            String result =  Optional.ofNullable(itemBsonObject.get("result")).map(val->val.toString()).orElse("");;
            String changeBriefResult =  Optional.ofNullable(itemBsonObject.get("changeBriefResult")).map(val->val.toString()).orElse("");;
            
            BSONObject myGnePointObject=(BSONObject)itemBsonObject.get("myGnePoint");
            BasicBSONList myGnePointsList = new BasicBSONList();
            if(myGnePointObject!=null)
                myGnePointsList.putAll(myGnePointObject);
            
            if (StringUtils.isNotBlank(id) && StringUtils.isNotBlank(name) && StringUtils.isNotBlank(result)) {
                
                String rowkey=id+":"+barcode;
                
                Put productPut=new Put(rowkey.getBytes());
                productPut.addColumn("info".getBytes(), "product".getBytes(), productId.getBytes());
                context.write(new Text(rowkey),productPut);
                
                Put namePut=new Put(rowkey.getBytes());
                namePut.addColumn("info".getBytes(), "name".getBytes(), name.getBytes());
                context.write(new Text(rowkey),namePut);
                
                Put resultPut=new Put(rowkey.getBytes());
                resultPut.addColumn("info".getBytes(), "result".getBytes(), result.getBytes());
                context.write(new Text(rowkey),resultPut);
                
                Put changeBriefResultPut=new Put(rowkey.getBytes());
                changeBriefResultPut.addColumn("info".getBytes(), "briefResult".getBytes(), changeBriefResult.getBytes());
                context.write(new Text(rowkey),changeBriefResultPut);
                
                if(myGnePointObject!=null)
                    reduceGene(context,myGnePointsList,rowkey);
            }
        }
    }
    
    private void reduceDiseaseResult(Context context,BasicBSONList list,String barcode,String productId) throws IOException, InterruptedException {
        
        for (Object object : list) {
            
            BSONObject itemBsonObject = (BSONObject)object;
            String id = Optional.ofNullable(itemBsonObject.get("_id")).map(val->val.toString()).orElse("");
            String name = Optional.ofNullable(itemBsonObject.get("name")).map(val->val.toString()).orElse("");
            String multiple =  Optional.ofNullable(itemBsonObject.get("multiple")).map(val->val.toString()).orElse("");
            String lrNum =  Optional.ofNullable(itemBsonObject.get("lrNum")).map(val->val.toString()).orElse("");
            String mutationAdvice =  Optional.ofNullable(itemBsonObject.get("mutationAdvice")).map(val->val.toString()).orElse("");
            String resultAdvice =  Optional.ofNullable(itemBsonObject.get("resultAdvice")).map(val->val.toString()).orElse("");
            
            BSONObject myGnePointObject=(BSONObject)itemBsonObject.get("myGnePoint");
            BasicBSONList myGnePointsList = new BasicBSONList();
            if(myGnePointObject!=null)
                myGnePointsList.putAll(myGnePointObject);
            
            if (StringUtils.isNotBlank(id) && StringUtils.isNotBlank(name)) {
                
                String rowkey=id+":"+barcode;
                
                /**
                 * 产品
                 */
                Put productPut=new Put(rowkey.getBytes());
                productPut.addColumn("info".getBytes(), "product".getBytes(), productId.getBytes());
                context.write(new Text(rowkey),productPut);
                
                /**
                 * 名字
                 */
                Put namePut=new Put(rowkey.getBytes());
                namePut.addColumn("info".getBytes(), "name".getBytes(), name.getBytes());
                context.write(new Text(rowkey),namePut);
                
                /**
                 * 倍数
                 */
                Put multiplePut=new Put(rowkey.getBytes());
                multiplePut.addColumn("info".getBytes(), "multiple".getBytes(), multiple.getBytes());
                context.write(new Text(rowkey),multiplePut);
                
                /**
                 * lr
                 */
                Put lrNumPut=new Put(rowkey.getBytes());
                lrNumPut.addColumn("info".getBytes(), "lr".getBytes(), lrNum.getBytes());
                context.write(new Text(rowkey),lrNumPut);
                
                /**
                 * 突变建议
                 */
                Put mutationAdvicePut=new Put(rowkey.getBytes());
                mutationAdvicePut.addColumn("info".getBytes(), "mutation".getBytes(), mutationAdvice.getBytes());
                context.write(new Text(rowkey),mutationAdvicePut);
                
                /**
                 * 结果建议
                 */
                Put resultAdvicePut=new Put(rowkey.getBytes());
                resultAdvicePut.addColumn("info".getBytes(), "result".getBytes(), resultAdvice.getBytes());
                context.write(new Text(rowkey),resultAdvicePut);
                if(myGnePointObject!=null)
                    reduceGene(context,myGnePointsList,rowkey);
            }
        }
    }
    
    /**
     * 获取基因型
     * @param myGnePointsList
     * @param rowKey
     * @return
     * @throws InterruptedException 
     * @throws IOException 
     */
    private void reduceGene(Context context,BasicBSONList myGnePointsList,String rowKey) throws IOException, InterruptedException{
        for(Object object : myGnePointsList) {
            Put put=new Put(rowKey.getBytes());
            BSONObject itemBsonObject = (BSONObject)object;
            
            put.addColumn("info".getBytes(),
                        itemBsonObject.get("point").toString().getBytes(),
                        itemBsonObject.get("genotype").toString().getBytes());
            context.write(new Text(rowKey), put);
        }
    }
}
