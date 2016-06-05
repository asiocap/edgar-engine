package com.edgarengine.kafka.pojo;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.json.JSONObject;

import static com.edgarengine.kafka.pojo.Utilities.getIntValue;
import static com.edgarengine.kafka.pojo.Utilities.getStringValue;

/**
 * @author Jincheng Chen
 */
@ThriftStruct
public class NonDerivativeHolding {
    @ThriftField(1)
    public int sharesOwnedFollowingTransaction;

    @ThriftField(2)
    public String directOrIndirectOwnership;

    @ThriftField(3)
    public String natureOfOwnership;

    @ThriftField(4)
    public String securityTitle;

    public NonDerivativeHolding() {}

    NonDerivativeHolding(JSONObject json) {

        if (json.has("postTransactionAmounts")) {
            JSONObject postTransactionAmounts = (JSONObject) json.get("postTransactionAmounts");
            sharesOwnedFollowingTransaction = getIntValue("sharesOwnedFollowingTransaction", postTransactionAmounts);
        }

        if (json.has("ownershipNature")) {
            JSONObject ownershipNature = (JSONObject) json.get("ownershipNature");
            directOrIndirectOwnership = getStringValue("directOrIndirectOwnership", ownershipNature);
            natureOfOwnership = getStringValue("natureOfOwnership", ownershipNature);
        }

        securityTitle = getStringValue("securityTitle", json);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("NonDerivativeHolding");

        sb.append("\n").append("sharesOwnedFollowingTransaction = ").append(sharesOwnedFollowingTransaction);
        sb.append("\n").append("directOrIndirectOwnership = ").append(directOrIndirectOwnership);
        sb.append("\n").append("natureOfOwnership = ").append(natureOfOwnership);
        sb.append("\n").append("securityTitle = ").append(securityTitle);

        return sb.toString();
    }
}
