package com.mofang.spark.vo;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * Created by manmao on 2018/4/4.
 * 
 * 用于统计删除日志
 * 
 */
public class RemoveLog implements Serializable{

    private static final long serialVersionUID = 1L;

    private String barcode;

    private int count;

    private List<String> items;

    private Date date;

    public String getBarcode() {
        return barcode;
    }

    public void setBarcode(String barcode) {
        this.barcode = barcode;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public List<String> getItems() {
        return items;
    }

    public void setItems(List<String> items) {
        this.items = items;
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
                ", count=" + count +
                ", items=" + items +
                ", date=" + date +
                '}';
    }
}
