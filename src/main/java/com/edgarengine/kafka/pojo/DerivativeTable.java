package com.edgarengine.kafka.pojo;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author Jincheng Chen
 */
@ThriftStruct
public class DerivativeTable {
    private static Logger LOG = Logger.getLogger(DerivativeTable.class.getName());

    @ThriftField(1)
    public List<DerivativeTransaction> transactions;

    @ThriftField(2)
    public List<DerivativeHolding> holdings;

    public DerivativeTable() {}

    DerivativeTable(JSONObject json) {
        if (json.has("derivativeTransaction")) {
            transactions = new ArrayList<DerivativeTransaction>();
            if (json.get("derivativeTransaction") instanceof  JSONArray) {
                JSONArray array = json.getJSONArray("derivativeTransaction");

                for (int i = 0; i < transactions.size(); i++) {
                    transactions.add(new DerivativeTransaction(array.getJSONObject(i)));
                }
            } else if (json.get("derivativeTransaction") instanceof JSONObject) {
                transactions.add(new DerivativeTransaction(json.getJSONObject("derivativeTransaction")));
            } else {
                LOG.severe(String.format("Unexpected json node type %s for derivativeTransaction",
                        json.get("derivativeTransaction").getClass().getCanonicalName()));
            }
        }

        if (json.has("derivativeHolding")) {
            holdings = new ArrayList<DerivativeHolding>();

            if (json.get("derivativeHolding") instanceof  JSONArray) {
                JSONArray array = json.getJSONArray("derivativeHolding");


                for (int i = 0; i < holdings.size(); i++) {
                    holdings.add(new DerivativeHolding(array.getJSONObject(i)));
                }
            } else if (json.get("derivativeHolding") instanceof JSONObject) {
                holdings.add(new DerivativeHolding(json.getJSONObject("derivativeHolding")));
            } else {
                LOG.severe(String.format("Unexpected json node type %s for derivativeHolding",
                        json.get("derivativeHolding").getClass().getCanonicalName()));
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DerivativeTable");

        if (transactions != null) {
            for (DerivativeTransaction transaction : transactions) {
                sb.append("\n").append(transaction.toString());
            }
        }

        if (holdings != null) {
            for (DerivativeHolding holding : holdings) {
                sb.append("\n").append(holding.toString());
            }
        }

        return sb.toString();
    }
}
