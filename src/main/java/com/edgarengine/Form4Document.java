package com.edgarengine;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.LinkedList;
import java.util.Stack;
import java.util.logging.Logger;

import com.mongodb.BasicDBObject;
import org.bson.*;
import org.bson.types.BasicBSONList;
import org.json.JSONObject;
import org.json.XML;
import org.xml.sax.SAXException;

/**
 * Created by jinchengchen on 4/29/16.
 */
public class Form4Document {
    private static Logger LOG = Logger.getLogger(Form4Document.class.getCanonicalName());
    private String file_path;
    private BasicDBObject mongo_doc;
    private Stack<BasicDBObject> track;
    private LinkedList<String> file_lines;
    private int number_of_documents;

    private Form4Document() {}

    public static Form4Document of(String file_path) {
        Form4Document instance = new Form4Document();
        instance.file_path = file_path;
        return instance;
    }

    public BasicDBObject parse() throws IOException, ParserConfigurationException, SAXException {

        Form4Document doc = this.initialize()
                .readOneLine("<SEC-DOCUMENT>")
                .readOneLine("<SEC-HEADER>")
                .readOneLine("<ACCEPTANCE-DATETIME>")
                .readOneLine("ACCESSION NUMBER:")
                .readOneLine("CONFORMED SUBMISSION TYPE:")
                .readDocumentCount("PUBLIC DOCUMENT COUNT:")
                .readOneLine("CONFORMED PERIOD OF REPORT:")
                .readOneLine("FILED AS OF DATE:")
                .readOneLine("DATE AS OF CHANGE:")
                .readIssuerAndReportingOwners()
                .ignoreKey("</SEC-HEADER>");

        for (int i = 0; i < this.number_of_documents; i++) {
            doc.readDocument();
        }

        return doc.ignoreKey("</SEC-DOCUMENT>").done();
    }

    private Form4Document initialize() throws IOException {
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
        return this;
    }

    private BasicDBObject done() throws IOException {
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

    private final Form4Document readOneLine(String key) throws IOException {
        return readOneLine(false, false, key);
    }

    private final Form4Document readOneLine(boolean optional, String key) throws IOException {
        return readOneLine(false, optional, key);
    }

    private Form4Document readOneLine(boolean strict_key, boolean optional, String key) throws IOException {
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

    private Form4Document readSection(String header, String... keys) throws UnsupportedEncodingException {
        return readSection(false, true, header, keys);
    }

    private Form4Document readSections(String header, String... keys) throws UnsupportedEncodingException {
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

    private Form4Document readSection(boolean optional, String header, String... keys)
            throws UnsupportedEncodingException {
        return readSection(false, optional, header, keys);
    }

    /**
     * @param strict_key
     * @param optional Whether it should throw @UnsupportedEncodingException if header could not be founded
     * @param header
     * @param keys Keys are all optional. However they should be a complete set in order.
     */
    private Form4Document readSection(boolean strict_key, boolean optional, String header, String... keys)
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

    private Form4Document readHeader(String key) throws IOException {
        return readHeader(key, false, false);
    }

    private Form4Document readHeader(String key, boolean strict_key, boolean optional) throws IOException {
        String pretty_key = key;
        if (!strict_key) {
            pretty_key = getPrettyKey(key);
        }

        String line = file_lines.poll();

        if (!line.equalsIgnoreCase(key)) {
            if (optional) {
                return this;
            } else {
                LOG.severe(String.format("Form 4 format exception on key %s, line %s, in %s", key, line ,file_path));
                throw new UnsupportedEncodingException();
            }
        }

        BasicDBObject child = new BasicDBObject();
        track.peek().put(pretty_key, child);
        track.push(child);
        return this;
    }

    private Form4Document completeHeader() {
        track.pop();
        return this;
    }

    private Form4Document ignoreKey(String key) throws IOException {
        return ignoreKey(true, key);
    }

    private Form4Document ignoreKey(boolean optional, String key) throws IOException {
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

    private Form4Document readDocumentCount(String key) throws UnsupportedEncodingException {
        String line = file_lines.poll();
        if (line == null || !line.contains(key)) {
            LOG.severe(String.format("Form 4 format exception on key %s line %s in %s", key, line, file_path));
            throw new UnsupportedEncodingException();
        }

        this.number_of_documents = Integer.valueOf(line.substring(key.length()).trim());
        return this;
    }

    private Form4Document readDocument() throws IOException, ParserConfigurationException, SAXException {
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

    private final Form4Document readIssuerAndReportingOwners() throws IOException {
        BasicBSONList reporting_owners = new BasicBSONList();
        BasicBSONList issuers = new BasicBSONList();
        while (file_lines.peek().equalsIgnoreCase("REPORTING-OWNER:") || file_lines.peek().equalsIgnoreCase("ISSUER:")) {
            if (file_lines.peek().equalsIgnoreCase("REPORTING-OWNER:")) {
                file_lines.poll();
                BasicDBObject owner = new BasicDBObject();
                reporting_owners.add(owner);
                track.push(owner);
                this.readSection("OWNER DATA:", "COMPANY CONFORMED NAME:", "CENTRAL INDEX KEY:",
                        "STANDARD INDUSTRIAL CLASSIFICATION:", "STATE OF INCORPORATION:", "FISCAL YEAR END:")
                        .readSection(true, "OWNER DATA:", "COMPANY CONFORMED NAME:", "CENTRAL INDEX KEY:",
                                "STANDARD INDUSTRIAL CLASSIFICATION:", "STATE OF INCORPORATION:", "FISCAL YEAR END:")
                        .readHeader("FILING VALUES:")
                        .readOneLine("FORM TYPE:")
                        .readOneLine("SEC ACT:")
                        .readOneLine("SEC FILE NUMBER:")
                        .readOneLine("FILM NUMBER:")
                        .completeHeader()
                        .readSection(true, "BUSINESS ADDRESS:", "STREET 1:", "STREET 2:", "CITY:", "STATE:", "ZIP:", "BUSINESS PHONE:")
                        .readSection("MAIL ADDRESS:", "STREET 1:", "STREET 2:", "CITY:", "STATE:", "ZIP:")
                        .readSections("FORMER NAME:", "FORMER CONFORMED NAME:", "DATE OF NAME CHANGE:");
                track.pop();
            } else {
                file_lines.poll();
                BasicDBObject issuer = new BasicDBObject();
                issuers.add(issuer);
                track.push(issuer);
                this.readSection("COMPANY DATA:", "COMPANY CONFORMED NAME:", "CENTRAL INDEX KEY:",
                                "STANDARD INDUSTRIAL CLASSIFICATION:", "IRS NUMBER:", "STATE OF INCORPORATION:", "FISCAL YEAR END:")
                        .readSection("BUSINESS ADDRESS:", "STREET 1:", "STREET 2:", "CITY:", "STATE:", "ZIP:", "BUSINESS PHONE:")
                        .readSection("MAIL ADDRESS:", "STREET 1:", "STREET 2:", "CITY:", "STATE:", "ZIP:")
                        .readSections("FORMER COMPANY:", "FORMER CONFORMED NAME:", "DATE OF NAME CHANGE:");
                track.pop();
            }
        }
        track.peek().put("REPORTING-OWNER", reporting_owners);
        track.peek().put("ISSUER:", issuers);
        return this;
    }

    private Form4Document readXML() throws IOException, ParserConfigurationException, SAXException {
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

    public static String getPrettyKey(String key) {
        if (key.endsWith(":")) {
            return key.substring(0, key.length() - 1).trim();
        }

        if (key.startsWith("<") && key.endsWith(">")) {
            return key.substring(1, key.length() - 1).trim();
        }

        return key;
    }

    public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {
        Form4Document.of("./data/edgar/data/1408356/0000899243-16-010872.txt").parse();
    }
}
