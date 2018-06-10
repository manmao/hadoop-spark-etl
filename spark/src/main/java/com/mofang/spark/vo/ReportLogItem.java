package com.mofang.spark.vo;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by manmao on 2018/4/4.
 * 
 * 日志项记录
 * 
 */
public class ReportLogItem implements Serializable{

    private static final long serialVersionUID = 1L;

    private String uniqId;  // 唯一标识， barcode+type+itemId
    
    private String type;    // 类型
    
    private String barcode; // 唾液盒
    
    private String itemId;  // 项目名

    private String content; // 日志内容

    private Date date;      // 操作时间

    public String getBarcode() {
        return barcode;
    }

    public void setBarcode(String barcode) {
        this.barcode = barcode;
    }
    
    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "RemoveItemCount{" +
                "barcode='" + barcode + '\'' +
                ", content=" + content +
                ", date=" + date +
                '}';
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUniqId() {
        return uniqId;
    }

    public void setUniqId(String uniqId) {
        this.uniqId = uniqId;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }
}
