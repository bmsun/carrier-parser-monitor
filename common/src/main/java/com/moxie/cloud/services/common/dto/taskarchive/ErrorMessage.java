package com.moxie.cloud.services.common.dto.taskarchive;


import lombok.Data;

@Data
public class ErrorMessage {
    //异常字段
    private String type;
    //异常内容
    private String message;
    //出现异常数
    private Integer errorNum;
    //总记录数
    private Integer totalNum;
    //异常率百分比
    private String rate;
}
