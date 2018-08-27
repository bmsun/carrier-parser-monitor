package com.moxie.cloud.services.common.dto;


import lombok.Data;

@Data
public class MessageInfo {
    /**
     * 是否压缩标识
     * 0：不压缩；1：压缩
     * */
    int flag;

    String parserMonitorInfo;

}
