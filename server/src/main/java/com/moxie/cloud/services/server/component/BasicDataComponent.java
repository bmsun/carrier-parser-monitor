package com.moxie.cloud.services.server.component;

import com.google.common.collect.Lists;
import com.moxie.cloud.carrier.entity.MobileEntity;
import com.moxie.cloud.carrier.entity.PeopleEntity;
import com.moxie.cloud.services.common.constants.DataMonitorConstants;
import com.moxie.cloud.services.common.dto.CarrierVerifty;
import com.moxie.cloud.services.common.dto.MobileDetailInfo;
import com.moxie.cloud.services.common.dto.taskarchive.ErrorMessage;
import com.moxie.cloud.services.common.dto.taskarchive.ParserMonitorMessage;
import com.moxie.cloud.services.common.enums.CodeEnum;
import com.moxie.cloud.services.common.enums.DataTypeEnum;
import com.moxie.cloud.services.common.metadata.CrawlerMetadataModel;
import com.moxie.cloud.services.common.utils.IdcardValidateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

@Slf4j
@Component("basicDataComponent")
public  class BasicDataComponent extends BaseComponent{


    @Override
    public String getType() {
        return DataTypeEnum.BASIC.name();
    }

    @Override
    public void verifyData(CrawlerMetadataModel crawlerMetadataModel, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage monitorMessage) {
        ParserMonitorMessage basicMessage = copyParserMonitorMessage(monitorMessage);
        basicMessage.setType(this.getType());

        MobileEntity mobileEntity = mobileDetailInfo.getMobileEntity();
        PeopleEntity peopleEntity = mobileDetailInfo.getPeopleEntity();
        verifyBasic(peopleEntity,mobileEntity,carrierVerify,keyPre,basicMessage);

    }

    private void verifyBasic(PeopleEntity people, MobileEntity mobileEntity, CarrierVerifty carrierVerifty, String keyPre, ParserMonitorMessage basicMessage) {
         List<ErrorMessage>  messageList = Lists.newArrayList();
         /** 实际上people,mobileEntity不会为空*/
        if (people != null) {
            // idcard
            if (isTrue(carrierVerifty.getIdCard())) {
                if (StringUtils.isBlank(people.getIdCard())) {
                    String key = keyPre+":"+ DataMonitorConstants.PEOPLE_IDCARD_BLANK;
                    String field = DataMonitorConstants.PEOPLE_IDCARD_BLANK;
                    /**指标告警*/
                    incrErrorCount(key, field);
                    addErrorMsg("idCard",DataMonitorConstants.PEOPLE_IDCARD_BLANK,messageList,1,1);
                } else if (!isIdCardValidate(people.getIdCard())) {
                    incrErrorCount(String.format("%s:%s",keyPre,DataMonitorConstants.PEOPLE_IDCARD_INVALID),DataMonitorConstants.PEOPLE_IDCARD_INVALID);
                    addErrorMsg("idCard",DataMonitorConstants.PEOPLE_IDCARD_INVALID,messageList,1,1);
                }
            }
            //name
            if (isTrue(carrierVerifty.getUserName())) {
                if (StringUtils.isBlank(people.getName())) {
                    incrErrorCount(String.format("%s:%s",keyPre,DataMonitorConstants.PEOPLE_NAME_BLANK),DataMonitorConstants.PEOPLE_NAME_BLANK);
                    addErrorMsg("trueName",DataMonitorConstants.PEOPLE_NAME_BLANK,messageList,1,1);

                } else if (!isNameValidate(people.getName())) {
                    incrErrorCount(String.format("%s:%s",keyPre,DataMonitorConstants.PEOPLE_NAME_INVALID),DataMonitorConstants.PEOPLE_NAME_INVALID);
                    addErrorMsg("trueName",DataMonitorConstants.PEOPLE_NAME_INVALID,messageList,1,1);
                }
            }
            //实名制状态
            if (isTrue(carrierVerifty.getRealNameStatus())) {
                if (StringUtils.isBlank(people.getRealNameStatus())) {
                    incrErrorCount(String.format("%s:%s",keyPre,DataMonitorConstants.PEOPLE_REALNAME_STATUS_BLANK),DataMonitorConstants.PEOPLE_REALNAME_STATUS_BLANK);
                    addErrorMsg("realNameStatus",DataMonitorConstants.PEOPLE_REALNAME_STATUS_BLANK,messageList,1,1);
                }
            }
        }

        if (mobileEntity != null) {
            if (isTrue(carrierVerifty.getOpenTime())) {
                //检查入网时间
                if (mobileEntity.getOpenTime() == null) {
                    incrErrorCount(String.format("%s:%s",keyPre,DataMonitorConstants.MOBILE_OPEN_TIME_BLANK),DataMonitorConstants.MOBILE_OPEN_TIME_BLANK);
                    addErrorMsg("openTime",DataMonitorConstants.MOBILE_OPEN_TIME_BLANK,messageList,1,1);
                } else if (mobileEntity.getOpenTime().after(new Date()) || mobileEntity.getOpenTime().before(new DateTime(2000, 1, 1, 0, 0).toDate())) {
                    incrErrorCount(String.format("%s:%s",keyPre,DataMonitorConstants.MOBILE_OPEN_TIME_INVALID),DataMonitorConstants.MOBILE_OPEN_TIME_INVALID);
                    addErrorMsg("openTime",DataMonitorConstants.MOBILE_OPEN_TIME_INVALID,messageList,1,1);
                }
            }

            //可用余额
            if (isTrue(carrierVerifty.getAvailableBalance())) {
                if (mobileEntity.getAvailableBalance() == null) {
                    incrErrorCount(String.format("%s:%s",keyPre,DataMonitorConstants.MOBILE_BALANCE_BLANK),DataMonitorConstants.MOBILE_BALANCE_BLANK);
                    addErrorMsg("availableBalance",DataMonitorConstants.MOBILE_BALANCE_BLANK,messageList,1,1);
                }
            }

            //状态
            if (isTrue(carrierVerifty.getState())) {
                if (StringUtils.isBlank(mobileEntity.getState())) {
                    incrErrorCount(String.format("%s:%s",keyPre,DataMonitorConstants.MOBILE_STATE_BLANK),DataMonitorConstants.MOBILE_STATE_BLANK);
                    addErrorMsg("state",DataMonitorConstants.MOBILE_STATE_BLANK,messageList,1,1);
                }
            }

            //星级
            if (isTrue(carrierVerifty.getStarLevel())) {
                if (StringUtils.isBlank(mobileEntity.getLevel())) {
                    incrErrorCount(String.format("%s:%s",keyPre,DataMonitorConstants.MOBILE_STAR_BLANK),DataMonitorConstants.MOBILE_STAR_BLANK);
                    addErrorMsg("starLevel",DataMonitorConstants.MOBILE_STAR_BLANK,messageList,1,1);

                } else if (!IS_CHINESE_REGEX.matcher(mobileEntity.getLevel()).matches()) {
                    incrErrorCount(String.format("%s:%s",keyPre,DataMonitorConstants.MOBILE_STAR_INVALID),DataMonitorConstants.MOBILE_STAR_INVALID);
                    addErrorMsg("starLevel",DataMonitorConstants.MOBILE_STAR_INVALID,messageList,1,1);
                }
            }
            //套餐名
            if (isTrue(carrierVerifty.getPackageName())){
                if (StringUtils.isBlank(mobileEntity.getPackageName())) {
                    incrErrorCount(String.format("%s:%s",keyPre,DataMonitorConstants.MOBILE_PACKAGE_NAME_BLANK),DataMonitorConstants.MOBILE_PACKAGE_NAME_BLANK);
                    addErrorMsg("packageName",DataMonitorConstants.MOBILE_PACKAGE_NAME_BLANK,messageList,1,1);
                }
            }
        }
        if(messageList.size()>0){
            //字段解析异常，count置为0
        buildMessage(0,basicMessage,CodeEnum.CODE_DATA_INVALIDE,messageList);
        addMessageToDb(basicMessage);
        }
    }



    /**
     * 姓名是否合法
     *
     * @param name
     * @return
     */
    public boolean isNameValidate(String name) {
        name = name.replace("*", "");
        name = name.replace("＊", "");
        name = name.replace("x", "");
        name = name.replace("（", "");
        name = name.replace("）", "");
        name = name.replace("(", "");
        name = name.replace(")", "");
        name = name.replaceAll("\\s", "");
        return IS_CHINESE_REGEX.matcher(name).matches();
    }

    /**
     * 证件号是否合法
     *
     * @param idcard
     * @return
     */
    public boolean isIdCardValidate(String idcard) {
        idcard = idcard.replace("\\s", "");
        if (idcard.contains("*")) {
            idcard = idcard.replace("*", "");
            return IS_CARD.matcher(idcard).matches();
        } else {
            return IdcardValidateUtils.isValidatedAllIdcard(idcard);
        }
    }

}
