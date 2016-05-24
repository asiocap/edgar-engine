package com.edgarengine.indexer;

/**
 *
 * @author Jincheng Chen
 */
public enum FileStatusEnum {
    EXCEPTION(-1, "exception"),
    NOT_FOUND(0, "not_found"),
    INITIALIZED(1, "initialized"),
    DOWNLOAD_FAILED(5, "download_failed"),
    DOWNLOADED(6, "downloaded"),
    PROCESSING(100, "processing"),
    PROCESSED(200, "processed"),

    ;

    public static String FIELD_KEY = "file_status";
    private final int id;
    private final String name;

    FileStatusEnum(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public static FileStatusEnum fromId(int id) {
        for (FileStatusEnum status : values()) {
            if (status.id == id) {
                return status;
            }
        }

        return null;
    }

    public int getId() {
        return id;
    }
}
