package com.mofang.data.componet;

import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import redis.clients.jedis.JedisPoolConfig;

public class RedisTemplateFactory {
    
    private static int REDIS_MAXIDLE=5;
    
    private static int REDIS_MAXTOTAL=20;
    
    private static boolean TESTONBORROW=false;
    
    private static String HOST_NAME="192.168.30.248";
    
    private static int PORT=7767;
    
    //private static String PASSWORD="";
    
    private static int TIME_OUT=2000;
    
    private static int DATABASE=8;
    
    public static StringRedisTemplate getRedisTemplate() {
        return RedisTemplateHolder.instance;
    }
    
    private static class RedisTemplateHolder{
        private static StringRedisTemplate instance=init();
    }
    
    private static StringRedisTemplate init() {
        
        JedisPoolConfig jedisPoolConfig=new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(REDIS_MAXIDLE);
        jedisPoolConfig.setMaxTotal(REDIS_MAXTOTAL);
        jedisPoolConfig.setTestOnBorrow(TESTONBORROW);
        
        JedisConnectionFactory jedisConnectionFactory=new JedisConnectionFactory();
        jedisConnectionFactory.setUsePool(true);
        jedisConnectionFactory.setHostName(HOST_NAME);
        jedisConnectionFactory.setPort(PORT);
        //jedisConnectionFactory.setPassword(PASSWORD);
        jedisConnectionFactory.setTimeout(TIME_OUT);
        jedisConnectionFactory.setDatabase(DATABASE);
        jedisConnectionFactory.setPoolConfig(jedisPoolConfig);
        jedisConnectionFactory.afterPropertiesSet();
        
        StringRedisTemplate stringRedisTemplate=new StringRedisTemplate();
        stringRedisTemplate.setConnectionFactory(jedisConnectionFactory);
        stringRedisTemplate.setDefaultSerializer(new StringRedisSerializer());
        stringRedisTemplate.afterPropertiesSet();
        
        return stringRedisTemplate;
    }
    
    
}
