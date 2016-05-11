package com.edgarengine.service;

import com.edgarengine.DataFileProcessor;
import com.edgarengine.indexer.CompanyIndexProcessor;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Created by jinchengchen on 5/1/16.
 */
public class HaidesServer {

    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        /**
         * 1. Download all new indexes files
         */
        RawDataCollector.INDEX_FILES_COLLECTOR.sync();

        /**
         * 2. Use company indexes to initiate data files metadata
         */

        new CompanyIndexProcessor().process();
        /**
         * 3. Generate machine readable data from Form 4 raw text files
         */

        new DataFileProcessor().processForm4();

        System.exit(0);

        /**
         * 4. Generate machine readable data from Form 8- raw text files
         */

//        new DataFileProcessor().processFormD();
    }
}
