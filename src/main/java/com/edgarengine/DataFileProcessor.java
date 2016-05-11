package com.edgarengine;

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
import org.json.JSONObject;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

import static com.edgarengine.service.RawDataCollector.GENERIC_FILES_COLLECTOR;

/**
 * Created by jinchengchen on 4/28/16.
 */
public class DataFileProcessor {
    private static Logger LOG = Logger.getLogger(DataFileProcessor.class.getCanonicalName());

    private final MongoCollection<BasicDBObject> data_files_collection;
    private final Form4MongoDao form4MongoDao = new Form4MongoDao();
    private final Properties produceerProps = new Properties();
    private Producer<String, Form4Object> producer;

    // TODO : temporary
    private Form4MongoDao formDMongoDao = new Form4MongoDao("haides", "form_d");

    public DataFileProcessor() {
        MongoClient mongoClient = new MongoClient("localhost" , 27017);
        MongoDatabase database = mongoClient.getDatabase("haides");
        data_files_collection = database.getCollection("data_files", BasicDBObject.class);

        // Kafka producer
        produceerProps.put("bootstrap.servers", "localhost:9092");
        produceerProps.put("acks", "all");
        produceerProps.put("retries", 0);
        produceerProps.put("batch.size", 16384);
        produceerProps.put("linger.ms", 1);
        produceerProps.put("buffer.memory", 33554432);
        produceerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        produceerProps.put("value.serializer", "com.edgarengine.kafka.SwiftSerializer");
    }

    public void processForm4() throws ParserConfigurationException, SAXException, IOException {
        BasicDBObject filter= new BasicDBObject();
        filter.put(DataFileSchema.FormType.field_name(), "4");
        filter.put(FileStatusEnum.FIELD_KEY, 1);
        FindIterable<BasicDBObject>  raw_files = data_files_collection.find(filter);

        for (BasicDBObject doc : raw_files) {
            processForm4(doc.getString(DataFileSchema.FileName.field_name()));
        }
    }

    private void processForm4(String file_name) throws ParserConfigurationException, SAXException, IOException {
        // Download it from FTP server if haven't done yet
        if (!GENERIC_FILES_COLLECTOR.sync(file_name)) {
            LOG.severe(String.format("Failed to sync file %s!", file_name));
            return;
        }

        // Tag this file as downloaded in DB
        BasicDBObject single_file_filter = new BasicDBObject(DataFileSchema.FileName.field_name(), file_name);
        BasicDBObject update = new BasicDBObject(FileStatusEnum.FIELD_KEY, FileStatusEnum.DOWNLOADED.getId());
        data_files_collection.findOneAndUpdate(single_file_filter, new BasicDBObject("$set", update));

        String local_file_path = GENERIC_FILES_COLLECTOR.getLocalPath() + File.separator + file_name;
        LOG.info(String.format("Start processing Form 4 data file %s", local_file_path));

        // Generate Json Object for general useage
        JSONObject json_object = XMLFormDocument.form4Of(local_file_path).parse();

        // Store in Mongo DB
        form4MongoDao.create(json_object);

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
            String file_name = doc.getString(DataFileSchema.FileName.field_name());
            if (GENERIC_FILES_COLLECTOR.sync(file_name)) {
                BasicDBObject single_file_filter = new BasicDBObject(DataFileSchema.FileName.field_name(), file_name);
                BasicDBObject update = new BasicDBObject(FileStatusEnum.FIELD_KEY, FileStatusEnum.DOWNLOADED.getId());
                data_files_collection.findOneAndUpdate(single_file_filter, new BasicDBObject("$set", update));

                String local_file_path = GENERIC_FILES_COLLECTOR.getLocalPath() + File.separator + file_name;
                LOG.info(local_file_path);

                JSONObject json_object = XMLFormDocument.formDOf(local_file_path).parse();
                formDMongoDao.create(json_object);
            }
        }
    }

    public Producer getProducer() {
        if (producer == null) {
            producer = new KafkaProducer<>(produceerProps);
        }

        return producer;
    }

    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        DataFileProcessor p = new DataFileProcessor();
        p.processForm4("edgar/data/1447935/16/000100307816000148/0001003078-16-000148.txt");
    }
}
