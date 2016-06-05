package com.edgarengine.kafka.pojo;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.json.JSONObject;

import java.util.logging.Logger;

import static com.edgarengine.kafka.pojo.Utilities.*;

/**
 * @author Jincheng Chen
 */
@ThriftStruct
public class NonDerivativeTransaction {
    private static Logger LOG = Logger.getLogger(NonDerivativeTransaction.class.getName());

    @ThriftField(1)
    public double transactionPricePerShare;

    @ThriftField(2)
    public String transactionAcquiredDisposedCode;

    @ThriftField(3)
    public int transactionShares;

    @ThriftField(4)
    public int sharesOwnedFollowingTransaction;

    @ThriftField(5)
    public String directOrIndirectOwnership;

    @ThriftField(6)
    public String natureOfOwnership;

    @ThriftField(7)
    public int transactionFormType;

    @ThriftField(8)
    public int equitySwapInvolved;

    @ThriftField(9)
    public String transactionCode;

    @ThriftField(10)
    public String transactionDate;

    @ThriftField(11)
    public String securityTitle;

    @ThriftField(12)
    public String deemedExecutionDate;

    @ThriftField(13)
    public String transactionTimeliness;

    public NonDerivativeTransaction() {}

    NonDerivativeTransaction(JSONObject json) {
        if (json.has("transactionAmounts")) {
            JSONObject transactionAmounts = (JSONObject) json.get("transactionAmounts");
            transactionPricePerShare = getDoubleValue("transactionPricePerShare", transactionAmounts);
            transactionAcquiredDisposedCode = getStringValue("transactionAcquiredDisposedCode", transactionAmounts);
            transactionShares = getIntValue("transactionShares", transactionAmounts);
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

        if (json.has("transactionCoding")) {
            JSONObject transactionCoding = (JSONObject) json.get("transactionCoding");
            transactionFormType = transactionCoding.has("transactionFormType")?
                    transactionCoding.getInt("transactionFormType") : 0;
            equitySwapInvolved = getInteger("equitySwapInvolved", transactionCoding);
            transactionCode = transactionCoding.has("transactionCode")?
                    transactionCoding.getString("transactionCode") : null;
        }

        transactionDate = getStringValue("transactionDate", json);
        securityTitle = getStringValue("securityTitle", json);
        deemedExecutionDate = getStringValue("deemedExecutionDate", json);
        transactionTimeliness = getStringValue("transactionTimeliness", json);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("NonDerivativeTransaction");

        sb.append("\n").append("transactionPricePerShare =").append(transactionPricePerShare);
        sb.append("\n").append("transactionAcquiredDisposedCode = ").append(transactionAcquiredDisposedCode);
        sb.append("\n").append("transactionShares = ").append(transactionShares);
        sb.append("\n").append("deemedExecutionDate = ").append(deemedExecutionDate);
        sb.append("\n").append("transactionTimeliness = ").append(transactionTimeliness);
        sb.append("\n").append("sharesOwnedFollowingTransaction = ").append(sharesOwnedFollowingTransaction);
        sb.append("\n").append("directOrIndirectOwnership = ").append(directOrIndirectOwnership);
        sb.append("\n").append("natureOfOwnership = ").append(natureOfOwnership);
        sb.append("\n").append("transactionFormType = ").append(transactionFormType);
        sb.append("\n").append("equitySwapInvolved = ").append(equitySwapInvolved);
        sb.append("\n").append("transactionCode = ").append(transactionCode);
        sb.append("\n").append("transactionDate = ").append(transactionDate);
        sb.append("\n").append("securityTitle = ").append(securityTitle);

        return sb.toString();
    }
}
