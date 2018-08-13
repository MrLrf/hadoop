package com.lirf.mongodb;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;

import static com.mongodb.client.model.Filters.*;

/**
 * 功能描述
 *
 * @author lirf
 * @Date 2017/9/7 14:44
 */
public class MongoUtil {
    private String ip = "127.0.0.1";
    private int port = 27017;

    private MongoClient getMongoClientSecret(String username, String password, String databaseName) {
        //连接到MongoDB服务 如果是远程连接可以替换“localhost”为服务器所在IP地址
        //ServerAddress()两个参数分别为 服务器地址 和 端口
        ServerAddress serverAddress = new ServerAddress(ip, port);
        List<ServerAddress> addresses = new ArrayList<>();
        addresses.add(serverAddress);

        //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
        MongoCredential credential = MongoCredential.createScramSha1Credential(username, databaseName, password.toCharArray());
        List<MongoCredential> credentials = new ArrayList<>();
        credentials.add(credential);

        //通过连接认证获取MongoDB连接
        MongoClient mongoClient = new MongoClient(addresses, credentials);
        return mongoClient;
    }

    private List<Map<String, Object>> mongoCollection2List(MongoCollection<Document> mongoCollection) {
        List<Map<String, Object>> result = new LinkedList<>();
        for (Document doc : mongoCollection.find()) {
            Map<String, Object> map = document2Map(doc);
            result.add(map);
        }
        return result;
    }

    public static Map<String, Object> document2Map(Document doc) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Document) {
                Document docValue = (Document) value;
                Map<String, Object> mapValue = document2Map(docValue);
                result.put(key, mapValue);
            } else {
                result.put(key, value);
            }
        }
        return result;
    }

    public List<Map<String, Object>> getCollectionData(String dbname, String collectionName) {
        MongoClient mongoClient = new MongoClient(ip, port);

        // 连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase(dbname);
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        List<Map<String, Object>> result = mongoCollection2List(collection);
        mongoClient.close();
        return result;
    }

    public Map<String, Object> getTopDocument(String dbName, String collectionName) {
//        List<Map<String, Object>> result = new ArrayList<>();
//
//        MongoClient mongoClient = new MongoClient(ip, port);
//
//        // 连接到数据库
//        MongoDatabase mongoDatabase = mongoClient.getDatabase(dbName);
//        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
//
//        Document document = collection.find().first();
//        mongoClient.close();
        return getDocumentByLimit(dbName, collectionName, 1).get(0);
    }

    public List<Map<String, Object>> getDocumentByLimit(String dbName, String collectionName, int count) {
        List<Map<String, Object>> result = new ArrayList<>();

        MongoClient mongoClient = new MongoClient(ip, port);

        // 连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase(dbName);
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);

        Block<Document> toListBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                result.add(document2Map(document));
            }
        };
        collection.find().limit(count).forEach(toListBlock);
        mongoClient.close();
        return result;
    }

    public List<Map<String, Object>> getDocumentByField(String dbName, String collectionName, String fieldName, String value) {
        List<Map<String, Object>> result = new ArrayList<>();

        MongoClient mongoClient = new MongoClient(ip, port);

        // 连接到数据库
        MongoDatabase mongoDatabase = mongoClient.getDatabase(dbName);
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);

        Block<Document> toListBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                result.add(document2Map(document));
            }
        };
        collection.find(and(eq(fieldName, value), gt("age", 20))).forEach(toListBlock);
        mongoClient.close();
        return result;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
