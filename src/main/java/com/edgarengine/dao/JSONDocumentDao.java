package com.edgarengine.dao;

import org.json.JSONObject;

/**
 * Created by jinchengchen on 5/10/16.
 */
public interface JSONDocumentDao {

    boolean create(JSONObject record);

    JSONObject get(Object key);

    int size();

    Iterable<JSONObject> select(JSONObject pattern);

}
