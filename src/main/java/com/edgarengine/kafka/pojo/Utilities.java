package com.edgarengine.kafka.pojo;

import org.json.JSONObject;

/**
 *
 * @author Jincheng Chen
 */
public class Utilities {
    private Utilities() {}

    public static int getIntValue(String field, JSONObject parentNode) {
        return getIntValue(field, parentNode, 0);
    }

    public static int getIntValue(String field, JSONObject parentNode, int defaultValue) {
        if (parentNode.has(field) && (parentNode.get(field) instanceof JSONObject) &&
                parentNode.getJSONObject(field).has("value")) {
            return parentNode.getJSONObject(field).getInt("value");
        }

        return defaultValue;
    }

    public static String getStringValue(String field, JSONObject parentNode) {
        if (parentNode.has(field) && (parentNode.get(field) instanceof JSONObject) &&
                parentNode.getJSONObject(field).has("value")) {
            return parentNode.getJSONObject(field).getString("value");
        }

        return null;
    }

    public static double getDoubleValue(String field, JSONObject parentNode) {
        return getDoubleValue(field, parentNode, 0.0f);
    }

    public static double getDoubleValue(String field, JSONObject parentNode, double defaultValue) {
        if (parentNode.has(field) && (parentNode.get(field) instanceof JSONObject) &&
                parentNode.getJSONObject(field).has("value")) {
            return parentNode.getJSONObject(field).getDouble("value");
        }

        return defaultValue;
    }

    public static int getInteger(String field, JSONObject node) {
        if (node.has(field)) {
            if (node.get(field) instanceof Boolean) {
                if (node.getBoolean(field)) {
                    return Integer.MAX_VALUE;
                } else {
                    return Integer.MIN_VALUE;
                }
            } else {
                return node.getInt(field);
            }
        }

        return 0;
    }
}