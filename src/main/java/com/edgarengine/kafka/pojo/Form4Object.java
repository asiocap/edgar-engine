package com.edgarengine.kafka.pojo;

import com.edgarengine.kafka.CompanyIndexed;

import java.util.*;
import java.util.logging.Logger;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.edgarengine.mongo.DataFileSchema.*;

/**
 * <p>
 * Thrift annotated POJO to represent Form 4.
 * </p>
 *
 * @author Jincheng Chen
 */
@ThriftStruct
public class Form4Object implements CompanyIndexed {
    private static Logger LOG = Logger.getLogger(Form4Object.class.getName());

    @ThriftField(4)
    public String companyName;

    @ThriftField(5)
    public long cik;

    @ThriftField(1)
    public String objectId;

    @ThriftField(2)
    public String accessNumber;

    @ThriftField(3)
    public String filedOfDate;

    @ThriftField(6)
    public String dateOfChange;

    @ThriftField(7)
    public DerivativeTable derivativeTable;

    @ThriftField(8)
    public NonDerivativeTable nonDerivativeTable;

    @ThriftField(9)
    public List<ReportingOwner> reportingOwners;


    // For Thrift deserialization purpose
    public Form4Object() {}

    public Form4Object(JSONObject object) {
        companyName = object.getString(CompanyName.field_name());
        cik = object.getLong(CIK.field_name());

        objectId = object.getString(_raw_file_path.field_name());
        accessNumber = object.getString(AccessionNumber.field_name());
        filedOfDate = object.getString(FiledAsOfDate.field_name());
        dateOfChange = object.getString(DateAsOfChange.field_name());

        JSONArray documents = object.getJSONArray("DOCUMENT");

        boolean xmlFound = false;
        for (int i = 0; i < documents.length(); i++) {
            JSONObject doc = (JSONObject) documents.get(i);
            if (doc.has("XML")) {
                if (xmlFound) {
                    LOG.severe(String.format("Multiple XML in Document in %s", objectId));
                    break;
                }
                xmlFound = true;

                JSONObject xml = (JSONObject) doc.get("XML");
                JSONObject ownershipDocument = (JSONObject) xml.get("ownershipDocument");
                if (ownershipDocument.has("derivativeTable") &&
                        ownershipDocument.get("derivativeTable") instanceof JSONObject) {
                    derivativeTable = new DerivativeTable((JSONObject) ownershipDocument.get("derivativeTable"));
                }

                if (ownershipDocument.has("nonDerivativeTable") &&
                        ownershipDocument.get("nonDerivativeTable") instanceof JSONObject) {
                    nonDerivativeTable = new NonDerivativeTable((JSONObject) ownershipDocument.get("nonDerivativeTable"));
                }

                if (ownershipDocument.has("reportingOwner")) {
                    reportingOwners = new ArrayList<ReportingOwner>();
                    if (ownershipDocument.get("reportingOwner") instanceof JSONArray) {
                        JSONArray array = ownershipDocument.getJSONArray("reportingOwner");

                        for (int j = 0; j < array.length(); j++) {
                            reportingOwners.add(new ReportingOwner(array.getJSONObject(j)));
                        }
                    } else if (ownershipDocument.get("reportingOwner") instanceof JSONObject) {
                        reportingOwners.add(new ReportingOwner(ownershipDocument.getJSONObject("reportingOwner")));
                    } else {
                        LOG.severe(String.format("Unexpected json node type %s for reportingOwner",
                                ownershipDocument.get("reportingOwner").getClass().getCanonicalName()));
                    }
                }
            }
        }
    }

    @Override
    public String getCompanyName() {
        return companyName;
    }

    @Override
    public long getCIK() {
        return cik;
    }

    public String getObjectId() {
        return objectId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("Form4");
        sb.append("\n").append("objectId =").append(objectId);

        sb.append("\n").append("objectId = ").append(objectId);
        sb.append("\n").append("accessNumber = ").append(accessNumber);
        sb.append("\n").append("filedOfDate = ").append(filedOfDate);
        sb.append("\n").append("dateOfChange = ").append(dateOfChange);
        sb.append("\n").append("objectId = ").append(objectId);

        if (derivativeTable != null) {
            sb.append("\n\n").append(derivativeTable.toString());
        }

        return sb.toString();
    }

    /**
     * Test Kafka consumer
     */
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.edgarengine.kafka.SwiftDeserializer");
        KafkaConsumer<String, Form4Object> consumer = new KafkaConsumer<>(props);


        TopicPartition[] partitions = new TopicPartition[1000];
        for (int p = 0; p < 1000; p++) {
            partitions[p] = new TopicPartition("form4-1", p);
        }

        consumer.assign(Arrays.asList(partitions));
        consumer.seekToBeginning(partitions);

        List<ConsumerRecord<String, Form4Object>> buffer = new ArrayList<>();
        int counter = 0;
        while (true) {
            ConsumerRecords<String, Form4Object> records = consumer.poll(100);
            for (ConsumerRecord<String, Form4Object> record : records) {
                System.out.printf("counter = %s, offset = %s, key = %s, value = %s\n",
                        counter, record.offset(), record.key(), record.value().toString());
                counter++;
                buffer.add(record);
            }
        }
    }
}

