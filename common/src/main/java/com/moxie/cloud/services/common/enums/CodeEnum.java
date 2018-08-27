package com.moxie.cloud.services.common.enums;

public enum CodeEnum {
    //本次未爬取到数据
    CODE_NO_DATA("0"),
    //采集失败
    CODE_CRAWL_ERROR("1"),
    //整体数据解析异常
    CODE_PARSER_ERROR("2"),
    //字段解析异常
    CODE_DATA_INVALIDE("3");

    String code;
    CodeEnum(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }



}
