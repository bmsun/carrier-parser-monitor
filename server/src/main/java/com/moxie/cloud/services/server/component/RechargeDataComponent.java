package com.moxie.cloud.services.server.component;

import com.google.common.collect.Lists;
import com.moxie.cloud.carrier.entity.MobileRechargeEntity;
import com.moxie.cloud.services.common.constants.DataMonitorConstants;
import com.moxie.cloud.services.common.dto.CarrierVerifty;
import com.moxie.cloud.services.common.dto.MobileDetailInfo;
import com.moxie.cloud.services.common.dto.taskarchive.ErrorMessage;
import com.moxie.cloud.services.common.dto.taskarchive.ParserMonitorMessage;
import com.moxie.cloud.services.common.enums.CodeEnum;
import com.moxie.cloud.services.common.enums.DataTypeEnum;
import com.moxie.cloud.services.common.metadata.CrawlerMetadataModel;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

@Component("rechargeDataComponent")
public class RechargeDataComponent extends BaseComponent {


    @Override
    public void verifyData(CrawlerMetadataModel crawlerMetadataModel, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage monitorMessage) {
        ParserMonitorMessage rechargeMessage = copyParserMonitorMessage(monitorMessage);
        rechargeMessage.setType(this.getType());
        Map<String, List<String>> rechargeCrawlerMap = getCrawlerMap(crawlerMetadataModel);
        if (rechargeCrawlerMap == null || rechargeCrawlerMap.isEmpty()) return;
        verifyRecharge(rechargeCrawlerMap, mobileDetailInfo, carrierVerify, keyPre, rechargeMessage);

    }

    private void verifyRecharge(Map<String, List<String>> crawlerMap, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage message) {
        List<MobileRechargeEntity> mobileRechargeList = mobileDetailInfo.getMobileRechargeList();
        /**充值信息校验不按月份区分*/
        if (CollectionUtils.isEmpty(mobileRechargeList)) {
            incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.RECHARGE_NO_DATA), DataMonitorConstants.RECHARGE_NO_DATA);
            ErrorMessage errorMessage = new ErrorMessage();
            errorMessage.setMessage(DataMonitorConstants.RECHARGE_NO_DATA);
            List<ErrorMessage> errorMessageList = Lists.newArrayList();
            errorMessageList.add(errorMessage);
            buildMessage(crawlerMap.size(), message, CodeEnum.CODE_NO_DATA, errorMessageList);
            //入库
            addMessageToDb(message);
            return;
        }
        verifyRechargeField(carrierVerify, keyPre, mobileRechargeList, message);

    }

    private void verifyRechargeField(CarrierVerifty carrierVerify, String keyPre, List<MobileRechargeEntity> mobileRechargeList, ParserMonitorMessage message) {

        int moneyErrorNum = 0;
        for (MobileRechargeEntity rechargeEntity : mobileRechargeList) {
            //金额高于500，记为异常
            if (rechargeEntity.getAmount() >= 50000) {
                moneyErrorNum++;
            }
        }
        List<ErrorMessage> messageList = Lists.newArrayList();
        if (moneyErrorNum > 0) {
            incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.RECHARGE_MONEY_EXTRACT_INVALID), DataMonitorConstants.RECHARGE_MONEY_EXTRACT_INVALID);
            addErrorMsg("sendType", DataMonitorConstants.RECHARGE_MONEY_EXTRACT_INVALID, messageList, moneyErrorNum, mobileRechargeList.size());
        }

        //入库
        if (messageList.size() > 0) {
            ParserMonitorMessage monitorMessage = copyParserMonitorMessage(message);
            buildMessage(0, monitorMessage, CodeEnum.CODE_DATA_INVALIDE, messageList);
            addMessageToDb(monitorMessage);
        }
    }

    @Override
    public String getType() {
        return DataTypeEnum.RECHARGE.name();
    }
}
