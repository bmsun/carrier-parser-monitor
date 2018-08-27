package com.moxie.cloud.services.common.dto;

import com.moxie.cloud.carrier.entity.*;
import com.moxie.commons.MoxieBeanUtils;
import lombok.Data;

import java.util.List;

@Data
public class BillModel {

    // 账单
    private BillEntity billEntity;

    // 账单明细
      //无用指标
    private List<Object> billItemList;

    // 语音通话集合
    private List<VoiceCallEntity> voiceCallList;

    // 短信集合
    private List<ShortMessageEntity> shortMessageList;

    // 套餐集合
    private List<PackageUsageEntity> packageUsageList;

    //流量集合
    private List<MobileNetFlowEntity> netFlowList;

    //上网记录集合
    List<SurfRecordEntity> surfRecordEntityList;
    @Override
    public String toString() {
        return MoxieBeanUtils.getJsonString(this);
    }

}
