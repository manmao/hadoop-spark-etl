package com.mofang.spark.mongo;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import com.mongodb.MongoClientURI;

public class MongoDBTemplateFactroy {

    private static  MongoTemplate mongoTemplate=null;

    public  static  MongoTemplate getMontoTemplate(String mongoUri) throws Exception {
         if(mongoTemplate == null){
             mongoTemplate=createMongoTemplate(mongoUri);
         }
         return mongoTemplate;
    }

    public static MongoTemplate createMongoTemplate(String mongoOutputUri) throws Exception {
        MongoClientURI mongoUri = new MongoClientURI(mongoOutputUri);
        SimpleMongoDbFactory dbFactory = new SimpleMongoDbFactory(mongoUri);
        return new MongoTemplate(dbFactory);
    }
}

