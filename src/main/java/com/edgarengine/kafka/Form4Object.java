package com.edgarengine.kafka;

import java.util.*;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;

/**
 * Created by jinchengchen on 5/6/16.
 */
@ThriftStruct
public class Form4Object {

    private String objectId;
    private String accessNumber = "1234";
    private String filedOfDate = "20160505";

    public static Form4Object of(String object_id) {
        return new Form4Object(object_id);
    }

    // For Thrift deserialization purpose
    public Form4Object() {}

    public Form4Object(JSONObject object) {
        objectId = object.getString("_raw_file_path");
        accessNumber = object.getString("ACCESSION NUMBER");
        filedOfDate = object.getString("FILED AS OF DATE");
    }

    private Form4Object(String objectId) {
        this.objectId = objectId;
    }

    @ThriftField(1)
    public String getObjectId() {
        return objectId;
    }

    @ThriftField(2)
    public String getAccessNumber() {
        return accessNumber;
    }

    @ThriftField(3)
    public String getFiledOfDate() {
        return filedOfDate;
    }

    @ThriftField(1)
    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }

    @ThriftField(2)
    public void setAccessNumber(String accessNumber) {
        this.accessNumber = accessNumber;
    }

    @ThriftField(3)
    public void setFiledOfDate(String filedOfDate) {
        this.filedOfDate = filedOfDate;
    }

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


        TopicPartition partition0 = new TopicPartition("form4-1", 0);
        consumer.assign(Arrays.asList(partition0));
        consumer.seekToBeginning(partition0);
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

