package com.edgarengine.documents;

import com.mongodb.BasicDBObject;
import org.bson.BSONObject;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.types.BasicBSONList;
import org.json.JSONObject;
import org.json.XML;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.Stack;
import java.util.logging.Logger;

/**
 * Created by jinchengchen on 5/5/16.
 */
public abstract class XMLFormDocument {
    private static Logger LOG = Logger.getLogger(XMLFormDocument.class.getCanonicalName());
    private String file_path;
    private BasicDBObject mongo_doc;
    private Stack<BasicDBObject> track;
    private LinkedList<String> file_lines;
    private int number_of_documents;

    public static Form4Document form4Of(String file_path) {
        Form4Document instance = new Form4Document();
        instance.setFilePath(file_path);
        return instance;
    }

    public static FormDDocument formDOf(String file_path) {
        FormDDocument instance = new FormDDocument();
        instance.setFilePath(file_path);
        return instance;
    }


    /**
     * Public methods
     */

    public BasicDBObject parse() throws IOException, ParserConfigurationException, SAXException {
        track = new Stack<BasicDBObject>();
        mongo_doc = new BasicDBObject();
        file_lines = new LinkedList<String>();
        number_of_documents = 0;

        track.push(mongo_doc);
        BufferedReader br = new BufferedReader(new FileReader(file_path));
        String line;
        while ((line = br.readLine()) != null) {
            if (line.trim().length() != 0) {
                file_lines.offer(line.trim());
            }
        }

        return this.readOneLine("<SEC-DOCUMENT>")
                .readOneLine("<SEC-HEADER>")
                .readOneLine("<ACCEPTANCE-DATETIME>")
                .readOneLine("ACCESSION NUMBER:")
                .readOneLine("CONFORMED SUBMISSION TYPE:")
                .readDocumentCount("PUBLIC DOCUMENT COUNT:")
                .readHeaders()
                .readRelatedPersons()
                .ignoreKey("</SEC-HEADER>")
                .readDocuments()
                .ignoreKey("</SEC-DOCUMENT>")
                .done();
    }

    /**
     * Common methods
     */

    void setFilePath(String file_path) {
        this.file_path = file_path;
    }

    BasicDBObject done() throws IOException {
        if (track.size() != 1) {
            LOG.severe(String.format("Form 4 format exception: unfinished stack in file %s!!", this.file_path));
            throw new UnsupportedEncodingException();
        }
        if (!file_lines.isEmpty()) {
            LOG.severe(String.format("Form 4 format exception: unfinished lines in file %s!", this.file_path));
            throw new UnsupportedEncodingException();
        }

        return mongo_doc;
    }

    final XMLFormDocument readOneLine(String key) throws IOException {
        return readOneLine(false, false, key);
    }

    final XMLFormDocument readOneLine(boolean optional, String key) throws IOException {
        return readOneLine(false, optional, key);
    }

    XMLFormDocument readOneLine(boolean strict_key, boolean optional, String key) throws IOException {
        String pretty_key = key;
        if (!strict_key) {
            pretty_key = getPrettyKey(key);
        }

        String line = file_lines.peek();
        if (line == null || !line.contains(key)) {
            if (!optional) {
                LOG.severe(String.format("Form 4 format exception on key %s line %s in %s", key, line, file_path));
                throw new UnsupportedEncodingException();
            }
        } else {
            track.peek().put(pretty_key, new BsonString(line.substring(key.length()).trim()));
            file_lines.poll();
        }
        return this;
    }

    XMLFormDocument readLines(String key) {
        String pretty_key = key;
        pretty_key = getPrettyKey(key);

        String line;
        BasicBSONList values = new BasicBSONList();
        while ((line = file_lines.peek()) != null) {
            if (line.startsWith(key)) {
                values.add(line.substring(key.length()).trim());
                file_lines.poll();
            } else {
                break;
            }
        }

        if (values.size() > 0) {
            track.peek().put(pretty_key, values);
        }
        return this;
    }

    XMLFormDocument readSection(String header, String... keys) throws UnsupportedEncodingException {
        return readSection(false, true, header, keys);
    }

    XMLFormDocument readSections(String header, String... keys) throws UnsupportedEncodingException {
        BsonArray sections_array = new BsonArray();
        while (file_lines.peek().equalsIgnoreCase(header)) {
            file_lines.poll();

            BsonDocument section = new BsonDocument();
            sections_array.add(section);

            // Keys
            for (String key : keys) {
                if (!file_lines.peek().startsWith(key)) {
                    continue;
                }
                String pretty_key = getPrettyKey(key);
                section.put(pretty_key, new BsonString(file_lines.poll().substring(key.length()).trim()));
            }
            sections_array.add(section);
        }

        if (sections_array.size() > 0) {
            track.peek().put(getPrettyKey(header), sections_array);
        }
        return this;
    }

    XMLFormDocument readSection(boolean optional, String header, String... keys)
            throws UnsupportedEncodingException {
        return readSection(false, optional, header, keys);
    }

    /**
     * @param strict_key
     * @param optional Whether it should throw @UnsupportedEncodingException if header could not be founded
     * @param header
     * @param keys Keys are all optional. However they should be a complete set in order.
     */
    XMLFormDocument readSection(boolean strict_key, boolean optional, String header, String... keys)
            throws UnsupportedEncodingException {
        // Find header first. If it is not found, throw exception unless it is optional.
        if (file_lines.peek().equalsIgnoreCase(header)) {
            file_lines.poll();
        } else if (optional) {
            return this;
        } else {
            LOG.severe(String.format("Form 4 format exception on header %s in %s", header, file_path));
            throw new UnsupportedEncodingException();
        }

        if (!strict_key) {
            header = getPrettyKey(header);
        }
        BsonDocument section = new BsonDocument();
        track.peek().put(header, section);

        // Keys
        for (String key : keys) {
            if (!file_lines.peek().startsWith(key)) {
                continue;
            }
            String pretty_key = key;
            if (!strict_key) {
                pretty_key = getPrettyKey(key);
            }
            section.put(pretty_key, new BsonString(file_lines.poll().substring(key.length()).trim()));
        }

        return this;
    }

    XMLFormDocument ignoreKey(String key) throws IOException {
        return ignoreKey(true, key);
    }

    XMLFormDocument ignoreKey(boolean optional, String key) throws IOException {
        String line = file_lines.peek();
        if (line == null || !line.contains(key)) {
            if (!optional) {
                LOG.severe(String.format("Form 4 format exception on key %s line %s in %s", key, line, file_path));
                throw new UnsupportedEncodingException();
            }
        } else {
            file_lines.poll();
        }

        return this;
    }

    XMLFormDocument readDocumentCount(String key) throws UnsupportedEncodingException {
        String line = file_lines.poll();
        if (line == null || !line.contains(key)) {
            LOG.severe(String.format("Form 4 format exception on key %s line %s in %s", key, line, file_path));
            throw new UnsupportedEncodingException();
        }

        this.number_of_documents = Integer.valueOf(line.substring(key.length()).trim());
        return this;
    }

    XMLFormDocument readDocuments() throws IOException, ParserConfigurationException, SAXException {
        for (int i = 0; i < this.number_of_documents; i++) {
            readDocument();
        }
        return this;
    }

    XMLFormDocument readDocument() throws IOException, ParserConfigurationException, SAXException {
        String line = file_lines.peek();
        if (line == null || !line.equalsIgnoreCase("<DOCUMENT>")) {
            file_lines.poll();
            file_lines.poll();
            LOG.severe(String.format("Form 4 format exception on DOCUMENT line %s in %s", line + "\n" + file_lines.peek(), file_path));
            throw new UnsupportedEncodingException();
        }

        file_lines.poll();
        if (!track.peek().containsField("DOCUMENT")) {
            track.peek().put("DOCUMENT", new BasicBSONList());
        }

        BasicDBObject document = new BasicDBObject();
        ((BasicBSONList)(track.peek().get("DOCUMENT"))).add(document);
        track.push(document);

        this.ignoreKey("<TYPE>")
                .readOneLine("<SEQUENCE>")
                .readOneLine("<FILENAME>")
                .readOneLine(true, "<DESCRIPTION>")
                .ignoreKey("<TEXT>");

        if (file_lines.peek().equalsIgnoreCase("<XML>")) {
            this.readXML();
        } else {
            StringBuilder text_content_builder = new StringBuilder();
            while(!file_lines.peek().equalsIgnoreCase("</TEXT>")) {
                text_content_builder.append(file_lines.poll()).append("\n");
            }
            document.put("TEXT", new BsonString(text_content_builder.toString()));
        }

        this.ignoreKey("</TEXT>")
                .ignoreKey("</DOCUMENT>");

        track.pop();
        return this;
    }
    XMLFormDocument readXML() throws IOException, ParserConfigurationException, SAXException {
        StringBuilder xml_builder = new StringBuilder();

        String line;
        while ((line = file_lines.poll()) != null) {
            line = line.trim();
            if (line.length() == 0) {
                continue;
            }
            if (line.equalsIgnoreCase("<XML>")) {
                continue;
            }
            if (line.equalsIgnoreCase("</XML>")) {
                break;
            }
            xml_builder.append(line);

        }
        JSONObject json_object = XML.toJSONObject(xml_builder.toString());
        BSONObject bson_xml = (BSONObject)com.mongodb.util.JSON.parse(json_object.toString());
        track.peek().put("XML", bson_xml);

        return this;
    }

    static String getPrettyKey(String key) {
        if (key.endsWith(":")) {
            return key.substring(0, key.length() - 1).trim();
        }

        if (key.startsWith("<") && key.endsWith(">")) {
            return key.substring(1, key.length() - 1).trim();
        }

        return key;
    }

    private final XMLFormDocument readRelatedPersons() throws IOException {
        boolean to_be_continued = true;
        while (to_be_continued) {
            to_be_continued = false;
            for (String name : getRelatedPersonNames()) {
                if (file_lines.peek().equalsIgnoreCase(name)) {
                    file_lines.poll();
                    if (!track.peek().containsField(name)) {
                        track.peek().put(name, new BasicBSONList());
                    }

                    BasicBSONList personList = (BasicBSONList) track.peek().get(name);
                    BasicDBObject person = new BasicDBObject();

                    personList.add(person);
                    track.push(person);

                    this.readSections("OWNER DATA:", "COMPANY CONFORMED NAME:", "CENTRAL INDEX KEY:",
                                "STANDARD INDUSTRIAL CLASSIFICATION:", "STATE OF INCORPORATION:", "FISCAL YEAR END:")
                            .readSections("COMPANY DATA:", "COMPANY CONFORMED NAME:", "CENTRAL INDEX KEY:",
                                "STANDARD INDUSTRIAL CLASSIFICATION:", "IRS NUMBER:", "STATE OF INCORPORATION:", "FISCAL YEAR END:")
                            .readSection(true, "FILING VALUES:", "FORM TYPE:", "SEC ACT:", "SEC FILE NUMBER:", "FILM NUMBER:")
                            .readSection(true, "BUSINESS ADDRESS:", "STREET 1:", "STREET 2:", "CITY:", "STATE:", "ZIP:", "BUSINESS PHONE:")
                            .readSection("MAIL ADDRESS:", "STREET 1:", "STREET 2:", "CITY:", "STATE:", "ZIP:")
                            .readSections("FORMER NAME:", "FORMER CONFORMED NAME:", "DATE OF NAME CHANGE:")
                            .readSections("FORMER COMPANY:", "FORMER CONFORMED NAME:", "DATE OF NAME CHANGE:");
                    track.pop();
                    to_be_continued = true;
                    break;
                }
            }
        }

        return this;
    }

    /**
     * Abstract methods
     */

    abstract XMLFormDocument readHeaders() throws IOException;

    abstract String[] getRelatedPersonNames();
}
