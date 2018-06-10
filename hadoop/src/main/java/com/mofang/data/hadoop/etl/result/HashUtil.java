package com.mofang.data.hadoop.etl.result;

public final class HashUtil {
    
    /**
     *  hash算法
     * @param data
     * @param len
     * @return
     */
    public final static int  murmurHash2(char data[], int len)  {  
        int  h, k,index=0;  
      
        h = 0 ^ len;  
      
        while (len >= 4) {  
            k  = data[index+0];  
            k |= data[index+1] << 8;  
            k |= data[index+2] << 16;  
            k |= data[index+3] << 24;  
      
            k *= 0x5bd1e995;  
            k ^= k >> 24;  
            k *= 0x5bd1e995;  
      
            h *= 0x5bd1e995;  
            h ^= k;  
      
            index += 4;  
            len -= 4;  
        }  
      
        switch (len) {
            case 3:
                h ^= data[2] << 16;
            case 2:
                h ^= data[1] << 8;
            case 1:
                h ^= data[0];
                h *= 0x5bd1e995;
        }
        
        h ^= h >> 13;  
        h *= 0x5bd1e995;  
        h ^= h >> 15;  
      
        return h;  
    }
    
    
    public final static String getRandomString(int length) {
        //随机字符串的随机字符库
        String KeyString = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuffer sb = new StringBuffer();
        int len = KeyString.length();
        for (int i = 0; i < length; i++) {
           sb.append(KeyString.charAt((int) Math.round(Math.random() * (len - 1))));
        }
        return sb.toString();
    }
}
