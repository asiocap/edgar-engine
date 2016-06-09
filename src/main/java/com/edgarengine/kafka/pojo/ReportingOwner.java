package com.edgarengine.kafka.pojo;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.json.JSONObject;

import static com.edgarengine.kafka.pojo.Utilities.getInteger;

/**
 * @author Jincheng Chen
 */
@ThriftStruct
public class ReportingOwner {

    // reportingOwnerId

    @ThriftField(1)
    public String rptOwnerName;

    @ThriftField(2)
    public String rptOwnerCik;

    // reportingOwnerAddress

    @ThriftField(3)
    public String rptOwnerCity;

    @ThriftField(4)
    public String rptOwnerStateDescription;

    @ThriftField(5)
    public String rptOwnerState;

    @ThriftField(6)
    public String rptOwnerZipCode;

    @ThriftField(7)
    public String rptOwnerStreet1;

    @ThriftField(8)
    public String rptOwnerStreet2;

    // reportingOwnerRelationship

    @ThriftField(9)
    public int isDirector;

    @ThriftField(10)
    public String otherText;

    @ThriftField(11)
    public int isOfficer;

    @ThriftField(12)
    public int isOther;

    @ThriftField(13)
    public int isTenPercentOwner;

    @ThriftField(14)
    public String officerTitle;

    public ReportingOwner() {}

    ReportingOwner(JSONObject json) {
        if (json.has("reportingOwnerId")) {
            JSONObject reportingOwnerId = json.getJSONObject("reportingOwnerId");
            rptOwnerName = reportingOwnerId.has("rptOwnerName") ? reportingOwnerId.getString("rptOwnerName") : null;
            rptOwnerCik = reportingOwnerId.has("rptOwnerCik") ? reportingOwnerId.getString("rptOwnerCik") : null;
        }

        if (json.has("reportingOwnerAddress")) {
            JSONObject reportingOwnerAddress = json.getJSONObject("reportingOwnerAddress");

            rptOwnerCity = reportingOwnerAddress.has("rptOwnerCity") ? reportingOwnerAddress.getString("rptOwnerCity") : null;
            rptOwnerStateDescription = reportingOwnerAddress.has("rptOwnerStateDescription") ?
                    reportingOwnerAddress.getString("rptOwnerStateDescription") : null;
            rptOwnerState = reportingOwnerAddress.has("rptOwnerState") ? reportingOwnerAddress.getString("rptOwnerState") : null;

            if (reportingOwnerAddress.has("rptOwnerZipCode")) {
                if (reportingOwnerAddress.get("rptOwnerZipCode") instanceof Integer) {
                    rptOwnerZipCode = Integer.toString(reportingOwnerAddress.getInt("rptOwnerZipCode"));
                } else if (reportingOwnerAddress.get("rptOwnerZipCode") instanceof String) {
                    rptOwnerZipCode = reportingOwnerAddress.getString("rptOwnerZipCode");
                }
            }

            rptOwnerStreet1 = reportingOwnerAddress.has("rptOwnerStreet1") ? reportingOwnerAddress.getString("rptOwnerStreet1") : null;
            rptOwnerStreet2 = reportingOwnerAddress.has("rptOwnerStreet2") ? reportingOwnerAddress.getString("rptOwnerStreet2") : null;
        }

        if (json.has("reportingOwnerRelationship")) {
            JSONObject reportingOwnerRelationship = json.getJSONObject("reportingOwnerRelationship");

            isDirector = getInteger("isDirector", reportingOwnerRelationship);
            otherText = reportingOwnerRelationship.has("otherText") ? reportingOwnerRelationship.getString("otherText") : null;
            isOfficer = getInteger("isOfficer", reportingOwnerRelationship);
            isOther = getInteger("isOther", reportingOwnerRelationship);
            isTenPercentOwner = getInteger("isTenPercentOwner", reportingOwnerRelationship);
            officerTitle = reportingOwnerRelationship.has("officerTitle") ? reportingOwnerRelationship.getString("officerTitle") : null;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("\nReporting Owner");

        sb.append("\n").append("rptOwnerName =").append(rptOwnerName);
        sb.append("\n").append("rptOwnerCik = ").append(rptOwnerCik);
        sb.append("\n").append("rptOwnerCity = ").append(rptOwnerCity);
        sb.append("\n").append("rptOwnerStateDescription = ").append(rptOwnerStateDescription);
        sb.append("\n").append("rptOwnerState = ").append(rptOwnerState);
        sb.append("\n").append("rptOwnerZipCode = ").append(rptOwnerZipCode);
        sb.append("\n").append("rptOwnerStreet1 = ").append(rptOwnerStreet1);
        sb.append("\n").append("rptOwnerStreet2 = ").append(rptOwnerStreet2);
        sb.append("\n").append("isDirector = ").append(isDirector);
        sb.append("\n").append("otherText = ").append(otherText);
        sb.append("\n").append("isOfficer = ").append(isOfficer);
        sb.append("\n").append("isOther = ").append(isOther);
        sb.append("\n").append("isTenPercentOwner = ").append(isTenPercentOwner);
        sb.append("\n").append("officerTitle = ").append(officerTitle);

        return sb.toString();
    }
}
