
package com.moxie.cloud.services.common.dto;

import com.moxie.cloud.carrier.entity.*;
import lombok.Data;

import java.util.List;

@Data
public class MobileDetailInfo {
    // 手机信息
    private MobileEntity mobileEntity;

    // 个人信息
    private PeopleEntity peopleEntity;
    // 充值
    private List<MobileRechargeEntity> mobileRechargeList;

    // 家庭网
    private List<FamilyNetEntity> familyNetList;

    private ExFieldEntity exFieldEntity;

    private List<BillModel> billModelList;
}
