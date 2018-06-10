package com.mofang.data.hadoop.etl.ratio;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class ItemResultWritable implements WritableComparable<ItemResultWritable> {
    
    private String id;
    
    private String name;
    
    private String result;
    
    
    
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

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public ItemResultWritable() {
        this.id="";
        this.name="";
        this.result="";
    }
    
    public ItemResultWritable(String id, String name, String result) {
        this.id = id;
        this.name = name;
        this.result =result;
    }
    
    @Override
    public void write(DataOutput out)
        throws IOException {
        out.writeUTF(id);
        out.writeUTF(name);
        out.writeUTF(result);
    }
    
    @Override
    public void readFields(DataInput in)
        throws IOException {
        id=in.readUTF();
        name=in.readUTF();
        result=in.readUTF();
    }
    
    @Override
    public int compareTo(ItemResultWritable o) {
        int cmp = id.compareTo(o.id);
        if (cmp != 0) { return cmp; }
        return name.compareTo(o.name);
    }

}
