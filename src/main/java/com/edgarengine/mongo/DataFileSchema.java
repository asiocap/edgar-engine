package com.edgarengine.mongo;

import com.edgarengine.indexer.FileStatusEnum;

/**
 * Created by jinchengchen on 4/28/16.
 */
public enum DataFileSchema {
    CompanyName("Company Name"),
    FormType("Form Type"),
    CIK("CIK"),
    DateFiled("Date Filed"),
    FileName("File Name"),
    FileStatus(FileStatusEnum.FIELD_KEY),
    _create_time_stamp("_create_time_stamp"),
    _index_file("_index_file"),

    ;

    private final String _field_name;
    DataFileSchema(String _field_name) {
        this._field_name = _field_name;
    }

    public String field_name() {
        return _field_name;
    }
}
