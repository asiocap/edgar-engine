package com.edgarengine.kafka.pojo;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author Jincheng Chen
 */
@ThriftStruct
public class NonDerivativeTable {
    private static Logger LOG = Logger.getLogger(NonDerivativeTable.class.getName());

    @ThriftField(1)
    public List<NonDerivativeTransaction> transactions;

    @ThriftField(2)
    public List<NonDerivativeHolding> holdings;

    public NonDerivativeTable() {}

    NonDerivativeTable(JSONObject json) {
        if (json.has("nonDerivativeTransaction")) {
            transactions = new ArrayList<NonDerivativeTransaction>();
            if (json.get("nonDerivativeTransaction") instanceof JSONArray) {
                JSONArray array = json.getJSONArray("nonDerivativeTransaction");

                for (int i = 0; i < array.length(); i++) {
                    transactions.add(new NonDerivativeTransaction(array.getJSONObject(i)));
                }
            } else if (json.get("nonDerivativeTransaction") instanceof JSONObject) {
                transactions.add(new NonDerivativeTransaction(json.getJSONObject("nonDerivativeTransaction")));
            } else {
                LOG.severe(String.format("Unexpected json node type %s for nonDerivativeTransaction",
                        json.get("nonDerivativeTransaction").getClass().getCanonicalName()));
            }
        }

        if (json.has("nonDerivativeHolding")) {
            holdings = new ArrayList<NonDerivativeHolding>();

            if (json.get("nonDerivativeHolding") instanceof  JSONArray) {
                JSONArray array = json.getJSONArray("nonDerivativeHolding");


                for (int i = 0; i < array.length(); i++) {
                    holdings.add(new NonDerivativeHolding(array.getJSONObject(i)));
                }
            } else if (json.get("nonDerivativeHolding") instanceof JSONObject) {
                holdings.add(new NonDerivativeHolding(json.getJSONObject("nonDerivativeHolding")));
            } else {
                LOG.severe(String.format("Unexpected json node type %s for nonDerivativeHolding",
                        json.get("nonDerivativeHolding").getClass().getCanonicalName()));
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("NonDerivativeTable");

        if (transactions != null) {
            for (NonDerivativeTransaction transaction : transactions) {
                sb.append("\n").append(transaction.toString());
            }
        }

        if (holdings != null) {
            for (NonDerivativeHolding holding : holdings) {
                sb.append("\n").append(holding.toString());
            }
        }

        return sb.toString();
    }
}
