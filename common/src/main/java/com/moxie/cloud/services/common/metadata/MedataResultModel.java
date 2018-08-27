package com.moxie.cloud.services.common.metadata;

import lombok.Data;


@Data
public class MedataResultModel {

    private String pageId;

    private String fileName;

    private String errorCode;

    private String errorMsg;
}
