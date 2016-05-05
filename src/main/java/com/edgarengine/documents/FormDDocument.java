package com.edgarengine.documents;

import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Created by jinchengchen on 5/5/16.
 */
public class FormDDocument extends XMLFormDocument {
    static final String[] RELATED_PERSON_NAMES = new String[] {"FILER:"};
    FormDDocument() {}

    XMLFormDocument readHeaders() throws IOException {
        return this.readLines("ITEM INFORMATION:")
                .readOneLine("FILED AS OF DATE:")
                .readOneLine("DATE AS OF CHANGE:")
                .readOneLine("EFFECTIVENESS DATE:");
    }

    String[] getRelatedPersonNames() {
        return RELATED_PERSON_NAMES;
    }

    public static void main(String[] args) throws ParserConfigurationException, SAXException, IOException {
        XMLFormDocument.formDOf("./data/edgar/data/1616646/0001209191-16-088336.txt").parse();
    }
}
