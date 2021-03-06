package com.edgarengine.mongo;

import com.edgarengine.indexer.FileStatusEnum;

/**
 *
 * @author Jincheng Chen
 */
public enum DataFileSchema {
    CompanyName("Company Name"),
    FormType("Form Type"),
    CIK("CIK"),
    DateFiled("Date Filed"),
    FileName("File Name"),
    FileStatus(FileStatusEnum.FIELD_KEY),
    AccessionNumber("ACCESSION NUMBER"),
    FiledAsOfDate("FILED AS OF DATE"),
    DateAsOfChange("DATE AS OF CHANGE"),

    _create_time_stamp("_create_time_stamp"),
    _index_file("_index_file"),
    _raw_file_path("_raw_file_path"),

    ;

    private final String _field_name;
    DataFileSchema(String _field_name) {
        this._field_name = _field_name;
    }

    public String field_name() {
        return _field_name;
    }
}
