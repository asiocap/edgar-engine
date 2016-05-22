package com.edgarengine.service;

import com.edgarengine.dao.Form4MongoDao;
import com.edgarengine.documents.XMLFormDocument;
import com.edgarengine.indexer.FileStatusEnum;
import com.edgarengine.kafka.Form4Object;
import com.edgarengine.mongo.DataFileSchema;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

import static com.edgarengine.mongo.DataFileSchema.CIK;
import static com.edgarengine.mongo.DataFileSchema.CompanyName;
import static com.edgarengine.mongo.DataFileSchema.FileName;
import static com.edgarengine.service.RawDataCollector.GENERIC_FILES_COLLECTOR;

/**
 * Created by jinchengchen on 4/28/16.
 */
public class DataFileProcessor {
    private static Logger LOG = Logger.getLogger(DataFileProcessor.class.getCanonicalName());

    private final MongoCollection<BasicDBObject> data_files_collection;
    private final MongoCollection<BasicDBObject> data_schemas_collection;
    private final Form4MongoDao form4MongoDao = new Form4MongoDao();
    private final Properties producerProps = new Properties();
    private Producer<String, Form4Object> producer;

    // TODO : temporary
    private Form4MongoDao formDMongoDao = new Form4MongoDao("haides", "form_d");

    public DataFileProcessor() {
        MongoClient mongoClient = new MongoClient("localhost" , 27017);
        MongoDatabase database = mongoClient.getDatabase("haides");
        data_files_collection = database.getCollection("data_files", BasicDBObject.class);
        data_schemas_collection = database.getCollection("data_schemas", BasicDBObject.class);

        // Kafka producer
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("acks", "all");
        producerProps.put("retries", 0);
        producerProps.put("batch.size", 16384);
        producerProps.put("linger.ms", 1);
        producerProps.put("buffer.memory", 33554432);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "com.edgarengine.kafka.SwiftSerializer");
        producerProps.put("partitioner.class", "com.edgarengine.kafka.CompanyPartitioner");
    }

    public void processForm4() throws ParserConfigurationException, SAXException, IOException {
        BasicDBObject filter= new BasicDBObject();
        filter.put(DataFileSchema.FormType.field_name(), "4");
        filter.put(FileStatusEnum.FIELD_KEY, 1);
        FindIterable<BasicDBObject> raw_files = data_files_collection.find(filter);

        for (BasicDBObject doc : raw_files) {
            processForm4(doc.getString(CompanyName.field_name()), doc.getString(CIK.field_name()), doc.getString(FileName.field_name()));
        }
    }

    private void processForm4(String company_name, String cik, String file_name) throws ParserConfigurationException, SAXException, IOException {
        // Download it from FTP server if haven't done yet
        if (!GENERIC_FILES_COLLECTOR.sync(file_name)) {
            LOG.severe(String.format("Failed to sync file %s!", file_name));
            return;
        }

        // Tag this file as downloaded in DB
        BasicDBObject single_file_filter = new BasicDBObject(FileName.field_name(), file_name);
        BasicDBObject update = new BasicDBObject(FileStatusEnum.FIELD_KEY, FileStatusEnum.DOWNLOADED.getId());
        data_files_collection.findOneAndUpdate(single_file_filter, new BasicDBObject("$set", update));

        String local_file_path = GENERIC_FILES_COLLECTOR.getLocalPath() + File.separator + file_name;
        LOG.info(String.format("Start processing Form 4 data file %s", local_file_path));

        // Generate Json Object for general useage
        JSONObject json_object = XMLFormDocument.form4Of(company_name, cik, local_file_path).parse();

        // Store in Mongo DB
        form4MongoDao.create(json_object);

        // Update Data Schema
        BasicDBObject schema = data_schemas_collection.find(new BasicDBObject("_type_", "form4")).first();
        if (schema == null) {
            schema = new BasicDBObject("_type_", "form4");
        }

        if (updateSchema(schema, json_object, "schema")) {
            data_schemas_collection.findOneAndDelete(new BasicDBObject("_type_", "form4"));
            data_schemas_collection.insertOne(schema);
        }

        // Send it to Kafka
        Form4Object serializableObject = new Form4Object(json_object);
        getProducer().send(new ProducerRecord<String, Form4Object>("form4-1", serializableObject.getObjectId(),
                serializableObject));
    }


    public void processFormD() throws ParserConfigurationException, SAXException, IOException {
        BasicDBObject filter= new BasicDBObject();
        filter.put(DataFileSchema.FormType.field_name(), "D");
        filter.put(FileStatusEnum.FIELD_KEY, 1);
        FindIterable<BasicDBObject>  raw_files = data_files_collection.find(filter);

        for (BasicDBObject doc : raw_files) {
            String file_name = doc.getString(FileName.field_name());
            String company_name = doc.getString(CompanyName.field_name());
            String cik = doc.getString(CIK.field_name());
            if (GENERIC_FILES_COLLECTOR.sync(file_name)) {
                BasicDBObject single_file_filter = new BasicDBObject(FileName.field_name(), file_name);
                BasicDBObject update = new BasicDBObject(FileStatusEnum.FIELD_KEY, FileStatusEnum.DOWNLOADED.getId());
                data_files_collection.findOneAndUpdate(single_file_filter, new BasicDBObject("$set", update));

                String local_file_path = GENERIC_FILES_COLLECTOR.getLocalPath() + File.separator + file_name;
                LOG.info(local_file_path);

                JSONObject json_object = XMLFormDocument.formDOf(company_name, cik, local_file_path).parse();
                formDMongoDao.create(json_object);
            }
        }
    }

    public Producer getProducer() {
        if (producer == null) {
            producer = new KafkaProducer<>(producerProps);
        }

        return producer;
    }

    public static boolean updateSchema(BasicDBObject schema, JSONObject data, String name) {
        boolean updated = false;
        if (!schema.containsField(name) || schema.get(name) instanceof String) {
            schema.put(name, new BasicDBObject());
        }
        BasicDBObject set = (BasicDBObject) schema.get(name);

        for (String key : data.keySet()) {
            Object node = data.get(key);
            if (node instanceof JSONObject) {
                updateSchema(set, (JSONObject) node, key);
            } else if (node instanceof JSONArray) {
                for(int i = 0; i < ((JSONArray) node).length(); i++) {
                    updateSchema(set, (JSONObject) ((JSONArray) node).get(i), key);
                }
            } else if (!set.containsField(key)) {
                set.put(key, node.getClass().getCanonicalName());
                updated = true;
            }
        }
        return updated;
    }

    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        DataFileProcessor p = new DataFileProcessor();
        p.processForm4("Company Name", "1447935", "edgar/data/1447935/16/000100307816000148/0001003078-16-000148.txt");
    }
}
