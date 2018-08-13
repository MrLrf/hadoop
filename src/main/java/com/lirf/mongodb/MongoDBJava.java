package com.lirf.mongodb;

import com.lirf.utils.ConfigUtil;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * 功能描述
 * @author lirf
 * @date 2017/12/9 9:56
 */
public class MongoDBJava {

    public static void main(String[] args) {
        String ip = (String) ConfigUtil.getMongodbConfig("ip");
        int port = Integer.parseInt((String) ConfigUtil.getMongodbConfig("port"));
        String databaseName = (String) ConfigUtil.getMongodbConfig("databaseName");

        //通过连接认证获取MongoDB连接
        MongoClient mongoClient = new MongoClient(ip, port);

        //连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
        System.out.println(mongoDatabase);

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("test2");
        System.out.println(mongoCollection.count());

        System.out.println(mongoCollection);
    }


}
