package com.moxie.cloud.services.common.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MetadateModel {

    /** 月份 yyyyMM */
    @JsonProperty("bill_date")
    private String billDate;

    private int status;

    private List<MedataResultModel> errors = new ArrayList<>();

}
