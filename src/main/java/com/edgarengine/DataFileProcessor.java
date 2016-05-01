package com.edgarengine;

import com.edgarengine.indexer.FileStatusEnum;
import com.edgarengine.mongo.DataFileSchema;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import static com.edgarengine.service.RawDataCollector.GENERIC_FILES_COLLECTOR;

/**
 * Created by jinchengchen on 4/28/16.
 */
public class DataFileProcessor {
    private static Logger LOG = Logger.getLogger(DataFileProcessor.class.getCanonicalName());
    private MongoCollection<BasicDBObject> data_files_collection;
    private MongoCollection<BasicDBObject> form_4_collection;

    public DataFileProcessor() {
        MongoClient mongoClient = new MongoClient("localhost" , 27017);
        MongoDatabase database = mongoClient.getDatabase("haides");
        data_files_collection = database.getCollection("data_files", BasicDBObject.class);
        form_4_collection= database.getCollection("form_4", BasicDBObject.class);
    }

    public void processForm4() throws ParserConfigurationException, SAXException, IOException {
        BasicDBObject filter= new BasicDBObject();
        filter.put(DataFileSchema.FormType.field_name(), "4");
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

                BasicDBObject object = Form4Document.of(local_file_path).parse();
                object.put("_raw_file_path", local_file_path);
                form_4_collection.insertOne(object);
            }
        }
    }

    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        DataFileProcessor p = new DataFileProcessor();
        p.processForm4();
    }
}
