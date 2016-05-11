package com.edgarengine.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.json.JSONObject;

/**
 * Created by jinchengchen on 5/10/16.
 */
public class Form4MongoDao implements JSONDocumentDao {
    private static final String HOST = "localhost";
    private static final int PORT = 27017;
    private static final String DB = "haides";
    private static final String COLLECTION = "form_4";

    private MongoCollection<BasicDBObject> form_4_collection;

    public Form4MongoDao() {
        this(DB, COLLECTION);
    }

    public Form4MongoDao(String db, String collection) {
        MongoClient mongoClient = new MongoClient(HOST , PORT);
        MongoDatabase database = mongoClient.getDatabase(db);
        form_4_collection= database.getCollection(collection, BasicDBObject.class);
    }

    @Override
    public boolean create(JSONObject record) {
        BasicDBObject object = (BasicDBObject)com.mongodb.util.JSON.parse(record.toString());
        form_4_collection.insertOne(object);
        return false;
    }

    @Override
    public JSONObject get(Object key) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Iterable<JSONObject> select(JSONObject pattern) {
        return null;
    }
}
