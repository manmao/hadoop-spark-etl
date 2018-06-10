package com.mofang.spark.log;


public enum LogType {
    
    ITEM_COUNT("item_count"),INHERIT("inherit"),REMOVE("remove");
    
    private String value;
    
    LogType(String value){
        this.setValue(value);
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
