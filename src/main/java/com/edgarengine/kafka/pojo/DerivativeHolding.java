package com.edgarengine.kafka.pojo;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.json.JSONObject;

import static com.edgarengine.kafka.pojo.Utilities.getDoubleValue;
import static com.edgarengine.kafka.pojo.Utilities.getIntValue;
import static com.edgarengine.kafka.pojo.Utilities.getStringValue;

/**
 *
 * @author Jincheng Chen
 */
@ThriftStruct
public class DerivativeHolding {

    @ThriftField(1)
    public int underlyingSecurityShares;

    @ThriftField(2)
    public String underlyingSecurityTitle;

    @ThriftField(3)
    public int sharesOwnedFollowingTransaction;

    @ThriftField(4)
    public String directOrIndirectOwnership;

    @ThriftField(5)
    public String natureOfOwnership;

    @ThriftField(6)
    public double conversionOrExercisePrice;

    @ThriftField(7)
    public String exerciseDate;

    @ThriftField(8)
    public String securityTitle;

    @ThriftField(9)
    public String expirationDate;

    public DerivativeHolding() {}

    DerivativeHolding(JSONObject json) {
        if (json.has("underlyingSecurity")) {
            JSONObject underlyingSecurity = (JSONObject) json.get("underlyingSecurity");
            underlyingSecurityShares = getIntValue("underlyingSecurityShares", underlyingSecurity);
            underlyingSecurityTitle = getStringValue("underlyingSecurityTitle", underlyingSecurity);
        }

        if (json.has("postTransactionAmounts")) {
            JSONObject postTransactionAmounts = (JSONObject) json.get("postTransactionAmounts");
            sharesOwnedFollowingTransaction = getIntValue("sharesOwnedFollowingTransaction", postTransactionAmounts);
        }

        if (json.has("ownershipNature")) {
            JSONObject ownershipNature = (JSONObject) json.get("ownershipNature");
            directOrIndirectOwnership = getStringValue("directOrIndirectOwnership", ownershipNature);
            natureOfOwnership = getStringValue("natureOfOwnership", ownershipNature);
        }

        conversionOrExercisePrice = getDoubleValue("conversionOrExercisePrice", json);
        exerciseDate = getStringValue("exerciseDate", json);
        securityTitle = getStringValue("securityTitle", json);
        expirationDate = getStringValue("expirationDate", json);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DerivativeHolding");

        sb.append("\n").append("underlyingSecurityShares =").append(underlyingSecurityShares);
        sb.append("\n").append("underlyingSecurityTitle = ").append(underlyingSecurityTitle);
        sb.append("\n").append("sharesOwnedFollowingTransaction = ").append(sharesOwnedFollowingTransaction);
        sb.append("\n").append("directOrIndirectOwnership = ").append(directOrIndirectOwnership);
        sb.append("\n").append("natureOfOwnership = ").append(natureOfOwnership);
        sb.append("\n").append("conversionOrExercisePrice = ").append(conversionOrExercisePrice);
        sb.append("\n").append("exerciseDate = ").append(exerciseDate);
        sb.append("\n").append("securityTitle = ").append(securityTitle);
        sb.append("\n").append("expirationDate = ").append(expirationDate);

        return sb.toString();
    }
}
