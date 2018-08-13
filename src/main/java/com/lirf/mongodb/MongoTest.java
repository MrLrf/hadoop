package com.lirf.mongodb;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;

/**
 * 功能描述
 *
 * @Author lirf
 * @Date 2017/9/7 14:47
 */
public class MongoTest {
    public static void main(String[] args) {
        MongoUtil mongoUtil = new MongoUtil();

        List<Map<String, Object>> result = new ArrayList<>();

        MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);

        // 连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase("local");
        MongoCollection<Document> collection = mongoDatabase.getCollection("test");

        Block<Document> toListBlock = document -> result.add(MongoUtil.document2Map(document));
//        collection.find(and( )).forEach(toListBlock);
//        mongoClient.close();
//        System.out.println(result);
    }

//    @Test
//    public void mongoTest() {
//        String searchQuery = "{\"type\":\"com.liuzg.jsweb.mongodb.query.AndExpression\"," +
//                "\"items\":[{\"type\":\"com.liuzg.jsweb.mongodb.query.EqExpression\",\"fieldName\":\"name\",\"value\":\"李瑞锋\"}," +
//                "{\"type\":\"com.liuzg.jsweb.mongodb.query.GtExpression\",\"fieldName\":\"age\",\"value\":20}]}";
//        //JSONObject jsonObject = new JSONObject(searchQuery);
//
//        ConditionExpressionFactory factory = new DefaultConditionExpressionFactory();
//        ConditionExpression expression = factory.createConditionExpression(searchQuery);
//
//        Bson bson = expression.toBson();
//        Bson value = Filters.and(eq("name", "李瑞锋"), gt("age", 20));
//
//        List<Map<String, Object>> result = getDocumentByField("local", "test", searchQuery);
//        System.out.println(result);
//    }
}
