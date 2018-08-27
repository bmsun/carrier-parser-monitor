package com.moxie.cloud.services.common.metadata;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 采集结果Metadata
*/
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CrawlerMetadataModel {

    private String type;

    @JsonProperty("type_name")
    private String typeName;

    /** 采集 */
    private List<MetadateModel> crawler = new ArrayList<>();

    /** 解析 */
    private List<MetadateModel> extractor = new ArrayList<>();

    /** 存储 */
    private List<MetadateModel> store = new ArrayList<>();
}
