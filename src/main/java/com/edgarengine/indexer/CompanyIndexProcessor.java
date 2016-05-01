package com.edgarengine.indexer;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.io.*;
import java.util.Date;
import java.util.logging.Logger;

import static com.edgarengine.service.RawDataCollector.INDEX_FILES_COLLECTOR;

/**
 * Created by jinchengchen on 4/26/16.
 */
public class CompanyIndexProcessor {
    private static Logger LOG = Logger.getLogger(CompanyIndexProcessor.class.getCanonicalName());

    private MongoCollection<BasicDBObject> company_index_collection;
    private MongoCollection<BasicDBObject> data_files_collection;

    public CompanyIndexProcessor() {
        MongoClient mongoClient = new MongoClient("localhost" , 27017);
        MongoDatabase database = mongoClient.getDatabase("haides");
        company_index_collection = database.getCollection("company_index_files", BasicDBObject.class);
        data_files_collection = database.getCollection("data_files", BasicDBObject.class);
    }

    public void process() {
        File local_index_files_folder = new File(INDEX_FILES_COLLECTOR.getLocalPath());
        if (!local_index_files_folder.exists()) {
            LOG.severe("Could not file index files folder!");
            return;
        }

        for (File file : local_index_files_folder.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (name.matches("company.*idx")) {
                    return true;
                }
                return false;
            }
        })) {
            String file_name = file.getName();
            FileStatusEnum status = get_status(file_name);
            boolean second_try = false;
            if (status == FileStatusEnum.PROCESSED) {
                continue;
            } else if (status == FileStatusEnum.PROCESSING) {
                second_try = true;
            } else if (status == FileStatusEnum.NOT_FOUND) {
                tag_as_processing(file_name);
            } else {
                LOG.severe(String.format("Unknown status %s while processing %s", status.getId(), file_name));
                return;
            }

            LOG.info("Processing " + file_name);
            try {
                if (process_index(file, second_try)) {
                    tag_as_processed(file_name);
                    LOG.info(String.format("Index file %s is processed!", file.getName()));
                }
            } catch (IOException e) {
                LOG.severe(String.format("Exception while processing file %s", file.getName()));
                e.printStackTrace();
            }
        }
    }

    private FileStatusEnum get_status(String file_name) {
        BsonDocument filter= new BsonDocument("file_name", new BsonString(file_name));
        if (company_index_collection.count(filter) > 1) {
            LOG.severe(String.format("More than one entries for the same file %s", file_name));
            return FileStatusEnum.EXCEPTION;
        }

        BasicDBObject doc = company_index_collection.find(filter).first();

        if (doc == null) {
            return FileStatusEnum.NOT_FOUND;
        }

        if (!doc.containsField(FileStatusEnum.FIELD_KEY)) {
             return FileStatusEnum.PROCESSING;
        }

        return FileStatusEnum.fromId(doc.getInt(FileStatusEnum.FIELD_KEY));
    }

    private void tag_as_processing(String file_name) {
        BasicDBObject doc = new BasicDBObject();
        doc.put("file_name", file_name);
        doc.put("create_date", new Date().toString());
        doc.put(FileStatusEnum.FIELD_KEY, FileStatusEnum.PROCESSING.getId());
        doc.put("last_modified_time", System.currentTimeMillis());
        company_index_collection.insertOne(doc);
    }

    private void tag_as_processed(String file_name) {
        BasicDBObject filter = new BasicDBObject("file_name", file_name);

        BasicDBObject update = new BasicDBObject();
        update.put(FileStatusEnum.FIELD_KEY, FileStatusEnum.PROCESSED.getId());
        update.put("last_modified_time", System.currentTimeMillis());

        company_index_collection.findOneAndUpdate(filter, new BasicDBObject("$set", update));
    }

    private boolean process_index(File file, boolean second_try) throws IOException {
        if (second_try) {
            data_files_collection.deleteMany(new BasicDBObject("_index_file", file.getName()));
        }

        BufferedReader br = new BufferedReader(new FileReader(file));

        String line;
        while ((line = br.readLine()) != null) {
            if (!line.contains("edgar/data")) {
                continue;
            }

            if (line.length() < 110) {
                LOG.severe(String.format("Company Index files format changed! \n%s", line));
                continue;
            }

            String company_name = line.substring(0, 62).trim();
            String form_type = line.substring(62, 74).trim();
            String cik = line.substring(74, 86).trim();
            String date_filed = line.substring(86, 98).trim();
            String file_name = line.substring(98).trim();

            if (!cik.matches("\\d*") || !date_filed.matches("\\d*") || !file_name.matches("edgar/data/.*txt")) {
                LOG.severe(String.format("Company Index files format changed! \n%s", line));
                continue;
            }


            BasicDBObject doc = new BasicDBObject();
            doc.put("Company Name", company_name);
            doc.put("Form Type", form_type);
            doc.put("CIK", cik);
            doc.put("Date Filed", date_filed);
            doc.put("File Name", file_name);

            doc.put(FileStatusEnum.FIELD_KEY, FileStatusEnum.INITIALIZED.getId());
            doc.put("_create_time_stamp", System.currentTimeMillis());
            doc.put("_index_file", file.getName());
            data_files_collection.insertOne(doc);
        }
        return true;
    }

    public static void main(String[] args) {
        CompanyIndexProcessor p = new CompanyIndexProcessor();
        p.process();
    }
}
