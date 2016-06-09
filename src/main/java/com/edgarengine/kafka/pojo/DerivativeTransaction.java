package com.edgarengine.kafka.pojo;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.json.JSONObject;

import java.util.logging.Logger;

import static com.edgarengine.kafka.pojo.Utilities.*;

/**
 *
 * @author Jincheng Chen
 */
@ThriftStruct
public class DerivativeTransaction {
    private static Logger LOG = Logger.getLogger(DerivativeTransaction.class.getName());

    @ThriftField(1)
    public int transactionPricePerShare;

    @ThriftField(2)
    public String transactionAcquiredDisposedCode;

    @ThriftField(3)
    public int transactionShares;

    @ThriftField(4)
    public String deemedExecutionDate;

    @ThriftField(5)
    public String transactionTimeliness;

    @ThriftField(6)
    public double underlyingSecurityShares;

    @ThriftField(7)
    public String underlyingSecurityTitle;

    @ThriftField(8)
    public int sharesOwnedFollowingTransaction;

    @ThriftField(9)
    public String directOrIndirectOwnership;

    @ThriftField(10)
    public String natureOfOwnership;

    @ThriftField(11)
    public double conversionOrExercisePrice;

    @ThriftField(12)
    public int transactionFormType;

    @ThriftField(13)
    public int equitySwapInvolved;

    @ThriftField(14)
    public String transactionCode;

    @ThriftField(15)
    public String exerciseDate;

    @ThriftField(16)
    public String transactionDate;

    @ThriftField(17)
    public String securityTitle;

    @ThriftField(18)
    public String expirationDate;

    @ThriftField(19)
    public int transactionTotalValue;

    public DerivativeTransaction() {}

    DerivativeTransaction(JSONObject json) {
        if (json.has("transactionAmounts")) {
            JSONObject transactionAmounts = (JSONObject) json.get("transactionAmounts");
            transactionPricePerShare = getIntValue("transactionPricePerShare", transactionAmounts);
            transactionAcquiredDisposedCode = getStringValue("transactionAcquiredDisposedCode", transactionAmounts);
            transactionShares = (int)getDoubleValue("transactionShares", transactionAmounts);
            transactionTotalValue = getIntValue("transactionTotalValue", transactionAmounts);
        }

        deemedExecutionDate = getStringValue("deemedExecutionDate", json);
        transactionTimeliness = getStringValue("transactionTimeliness", json);

        if (json.has("underlyingSecurity")) {
            JSONObject underlyingSecurity = (JSONObject) json.get("underlyingSecurity");
            underlyingSecurityShares = getDoubleValue("underlyingSecurityShares", underlyingSecurity);
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

        if (json.has("transactionCoding")) {
            JSONObject transactionCoding = (JSONObject) json.get("transactionCoding");
            transactionFormType = transactionCoding.has("transactionFormType")?
                    transactionCoding.getInt("transactionFormType") : 0;
            equitySwapInvolved = getInteger("equitySwapInvolved", transactionCoding);
            transactionCode = transactionCoding.has("transactionCode")?
                    transactionCoding.getString("transactionCode") : null;
        }

        exerciseDate = getStringValue("exerciseDate", json);
        transactionDate = getStringValue("transactionDate", json);
        securityTitle = getStringValue("securityTitle", json);
        expirationDate = getStringValue("expirationDate", json);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DerivativeTransaction");

        sb.append("\n").append("transactionPricePerShare =").append(transactionPricePerShare);
        sb.append("\n").append("transactionAcquiredDisposedCode = ").append(transactionAcquiredDisposedCode);
        sb.append("\n").append("transactionShares = ").append(transactionShares);
        sb.append("\n").append("deemedExecutionDate = ").append(deemedExecutionDate);
        sb.append("\n").append("transactionTimeliness = ").append(transactionTimeliness);
        sb.append("\n").append("underlyingSecurityShares = ").append(underlyingSecurityShares);
        sb.append("\n").append("underlyingSecurityTitle = ").append(underlyingSecurityTitle);
        sb.append("\n").append("sharesOwnedFollowingTransaction = ").append(sharesOwnedFollowingTransaction);
        sb.append("\n").append("directOrIndirectOwnership = ").append(directOrIndirectOwnership);
        sb.append("\n").append("natureOfOwnership = ").append(natureOfOwnership);
        sb.append("\n").append("conversionOrExercisePrice = ").append(conversionOrExercisePrice);
        sb.append("\n").append("transactionFormType = ").append(transactionFormType);
        sb.append("\n").append("equitySwapInvolved = ").append(equitySwapInvolved);
        sb.append("\n").append("transactionCode = ").append(transactionCode);
        sb.append("\n").append("exerciseDate = ").append(exerciseDate);
        sb.append("\n").append("transactionDate = ").append(transactionDate);
        sb.append("\n").append("securityTitle = ").append(securityTitle);
        sb.append("\n").append("expirationDate = ").append(expirationDate);

        return sb.toString();
    }
}
