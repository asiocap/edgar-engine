package com.edgarengine.documents;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;

import org.xml.sax.SAXException;

/**
 * Created by jinchengchen on 4/29/16.
 */
public class Form4Document extends XMLFormDocument {
    static final String[] RELATED_PERSON_NAMES = new String[] {"REPORTING-OWNER:", "ISSUER:"};

    Form4Document(String company_name, String cik, String file_path) {
        super(company_name, cik, file_path);
    }

    XMLFormDocument readHeaders() throws IOException {
        return this.readOneLine(true, "CONFORMED PERIOD OF REPORT:")
                .readOneLine("FILED AS OF DATE:")
                .readOneLine("DATE AS OF CHANGE:");
    }

    String[] getRelatedPersonNames() {
        return RELATED_PERSON_NAMES;
    }

    public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {
        XMLFormDocument.form4Of("Company Name", "1393726", "./data/edgar/data/1567756/0001127602-16-036823.txt").parse();
    }
}
