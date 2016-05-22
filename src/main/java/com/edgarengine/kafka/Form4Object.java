package com.edgarengine.kafka;

import java.util.*;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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




    // For Thrift deserialization purpose
    public Form4Object() {}

    public Form4Object(JSONObject object) {
        companyName = object.getString(CompanyName.field_name());
        cik = object.getLong(CIK.field_name());

        objectId = object.getString(_raw_file_path.field_name());
        accessNumber = object.getString(AccessionNumber.field_name());
        filedOfDate = object.getString(FiledAsOfDate.field_name());
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
                System.out.printf("counter = %s, offset = %s, key = %s, objectId = %s, filedOfDate = %s accessNumber = %s\n",
                        counter, record.offset(), record.key(), record.value().objectId, record.value().filedOfDate,
                        record.value().accessNumber);
                counter++;
                buffer.add(record);
            }
        }
    }
}

