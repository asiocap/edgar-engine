package com.edgarengine.service;

import com.edgarengine.utilities.ftp.MirrorMaker;

import java.io.File;
import java.util.logging.Logger;

/**
 *
 * @author Jincheng Chen
 */
public enum RawDataCollector {
    INDEX_FILES_COLLECTOR("/edgar/daily-index", true, false),
    GENERIC_FILES_COLLECTOR(new String(), true, false) {
        @Override
        public void sync() {
            LOG.warning("GENERIC_FILES_COLLECTOR is not used for bulk clone!");
            return;
        }
    },

    ;

    private static Logger LOG = Logger.getLogger(MirrorMaker.class.getName());
    public static final String FTP_SERVER_URL = "ftp.sec.gov";
    public static final String FTP_SERVER_USERNAME = "anonymous";
    public static final String FTP_SERVER_PASSWORD = "loocalvinci@gmail.com";


    public static final String DATA_LOCAL_ROOT = "./data";

    private final MirrorMaker mirror_maker;
    private final String path;
    private final boolean soft;
    private final boolean deep_clone;

    RawDataCollector(String path, boolean soft, boolean deep_clone) {
        mirror_maker = new MirrorMaker(FTP_SERVER_URL, FTP_SERVER_USERNAME, FTP_SERVER_PASSWORD, DATA_LOCAL_ROOT);

        this.path = path;
        this.soft = soft;
        this.deep_clone = deep_clone;
    }

    public String getPath() {
        return path;
    }

    public String getLocalPath() {
        return DATA_LOCAL_ROOT + path;
    }

    public void sync() {
        mirror_maker.replicateDirectory(path, soft, deep_clone);
    }

    public boolean sync(String file_name) {
        return mirror_maker.replicateFile(path + File.separator + file_name, soft);
    }

    public static void main(String[] args) {
        INDEX_FILES_COLLECTOR.sync();
    }
}
