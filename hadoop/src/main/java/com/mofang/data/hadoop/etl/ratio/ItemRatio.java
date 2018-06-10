package com.mofang.data.hadoop.etl.ratio;

import java.util.List;

public class ItemRatio {
    
    private String id;
    
    private String name;
    
    private List<ResultRetio> resultRetios;// 体质特征结果的对应比例
    
    private int  retioCount; //生成百分比时的样本总数
    
    
    static class ResultRetio{
        
        private String result;
        
        private float ratio;
        
        private int count;
        
      
        public float getRatio() {
            return ratio;
        }
        public void setRatio(float ratio) {
            this.ratio = ratio;
        }
        public int getCount() {
            return count;
        }
        public void setCount(int count) {
            this.count = count;
        }
        public String getResult() {
            return result;
        }
        public void setResult(String result) {
            this.result = result;
        }
        
    }


    public String getId() {
        return id;
    }


    public void setId(String id) {
        this.id = id;
    }


    public String getName() {
        return name;
    }


    public void setName(String name) {
        this.name = name;
    }


    public List<ResultRetio> getResultRetios() {
        return resultRetios;
    }


    public void setResultRetios(List<ResultRetio> resultRetios) {
        this.resultRetios = resultRetios;
    }


    public int getRetioCount() {
        return retioCount;
    }


    public void setRetioCount(int retioCount) {
        this.retioCount = retioCount;
    }
    
    
}
