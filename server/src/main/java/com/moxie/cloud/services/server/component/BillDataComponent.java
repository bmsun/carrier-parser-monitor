package com.moxie.cloud.services.server.component;

import com.google.common.collect.Lists;
import com.moxie.cloud.carrier.entity.BillEntity;
import com.moxie.cloud.services.common.constants.DataMonitorConstants;
import com.moxie.cloud.services.common.dto.BillModel;
import com.moxie.cloud.services.common.dto.CarrierVerifty;
import com.moxie.cloud.services.common.dto.MobileDetailInfo;
import com.moxie.cloud.services.common.dto.taskarchive.ErrorMessage;
import com.moxie.cloud.services.common.dto.taskarchive.ParserMonitorMessage;
import com.moxie.cloud.services.common.enums.CodeEnum;
import com.moxie.cloud.services.common.enums.DataTypeEnum;
import com.moxie.cloud.services.common.metadata.CrawlerMetadataModel;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component("billDataComponent")
@Slf4j
public class BillDataComponent extends BaseComponent {


    @Override
    public void verifyData(CrawlerMetadataModel crawlerMetadataModel, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage monitorMessage) {
        ParserMonitorMessage billMonitorMessage = copyParserMonitorMessage(monitorMessage);
        billMonitorMessage.setType(this.getType());

        Map<String, List<String>> billCrawlerMap = getCrawlerMap(crawlerMetadataModel);
        if (billCrawlerMap == null || billCrawlerMap.isEmpty()) return;
        verifyBills(billCrawlerMap, mobileDetailInfo, carrierVerify, keyPre, billMonitorMessage);

    }

    private void verifyBills(Map<String, List<String>> billCrawlerMap, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage billMonitorMessage) {
        List<BillModel> billModelList = mobileDetailInfo.getBillModelList();
        if (!CollectionUtils.isEmpty(billModelList)) {
            //按月分组，得到不为空的BillEntity
            Map<String, BillEntity> billEntityMap = billModelList.stream()
                    .map(BillModel::getBillEntity)
                    .filter(v -> v != null && v.getBillStartDate() != null
                            && (v.getTotalFee() != 0 || v.getActualFee() != 0))
                    .collect(Collectors.toMap(v -> formatMonth(v.getBillStartDate()), Function.identity(), (v, id) -> v));

            if (CollectionUtils.isEmpty(billEntityMap)) {
                handleNoResult(billCrawlerMap, keyPre, billMonitorMessage);
                return;
            }

            int crawlErrorNum = 0;
            int parserErrorNum = 0;
            for (Map.Entry<String, List<String>> entry : billCrawlerMap.entrySet()) {
                List<String> value = entry.getValue();

                boolean billFailflag = billEntityMap.get(entry.getKey()) == null
                        || (billEntityMap.get(entry.getKey()).getActualFee() == 0
                        && billEntityMap.get(entry.getKey()).getTotalFee() == 0);
                if (    //一个文件包含多个月份数据采集失败的情况不会走到此逻辑，无需加entry.getKey()=MX_ONE的判断
                        billFailflag && value.size() == 1
                                //采集失败 CACR-21301-30
                                && value.get(0).equals(CRAWL_FAIL_CODE)) {
                    crawlErrorNum++;
                } else if (//排除一个文件包含多个月份数据的情况
                        !entry.getKey().equals(MX_ONE) && billFailflag) {
                    parserErrorNum++;
                }
            }
            if (crawlErrorNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.BILL_CRAWL_ERROR), DataMonitorConstants.BILL_CRAWL_ERROR);
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(billMonitorMessage);
                ErrorMessage errorMessage = new ErrorMessage();
                errorMessage.setMessage(DataMonitorConstants.BILL_CRAWL_ERROR);
                List<ErrorMessage> errorMessageList = Lists.newArrayList();
                errorMessageList.add(errorMessage);
                buildMessage(crawlErrorNum, monitorMessage, CodeEnum.CODE_CRAWL_ERROR, errorMessageList);
                addMessageToDb(monitorMessage);
            }
            if (parserErrorNum > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.BILL_MONTH_EXTRACT_ERROR), DataMonitorConstants.BILL_MONTH_EXTRACT_ERROR);
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(billMonitorMessage);
                ErrorMessage errorMessage = new ErrorMessage();
                errorMessage.setMessage(DataMonitorConstants.BILL_MONTH_EXTRACT_ERROR);
                List<ErrorMessage> errorMessageList = Lists.newArrayList();
                errorMessageList.add(errorMessage);
                buildMessage(parserErrorNum, monitorMessage, CodeEnum.CODE_PARSER_ERROR, errorMessageList);
                addMessageToDb(monitorMessage);
            }

            //具体字段校验
            verifyBillField(billEntityMap, keyPre,billMonitorMessage);

        } else {
            handleNoResult(billCrawlerMap, keyPre, billMonitorMessage);
        }
    }

    private void verifyBillField(Map<String, BillEntity> billEntityMap, String keyPre, ParserMonitorMessage billMonitorMessage) {
        if (!CollectionUtils.isEmpty(billEntityMap)) {
            int billEndDateBeforStartError = 0,
                    billEndDateEqualStartError = 0,
                    billStartDateNotFirstDayError = 0,
                    billBaseFeeError = 0,
                    billTotalFeeError = 0;

            for (Map.Entry<String, BillEntity> entry : billEntityMap.entrySet()) {
                BillEntity billEntity = entry.getValue();
                //账单日期
                if (billEntity.getBillEndDate().before(billEntity.getBillStartDate())) {
                    billEndDateBeforStartError++;
                } else if (billEntity.getBillEndDate().equals(billEntity.getBillStartDate())) {
                    billEndDateEqualStartError++;
                } else if (!new DateTime(billEntity.getBillStartDate()).toString("yyyyMMdd").endsWith("01")) {
                    billStartDateNotFirstDayError++;
                }

                //账单基本套餐费 <=0
                if (billEntity.getBaseFee() <= 0) {
                    billBaseFeeError++;
                }

                //账单各项费用合计与总额不等
                if ((billEntity.getBaseFee() + billEntity.getSmsFee() + billEntity.getVasFee() + billEntity.getVoiceFee()
                        + billEntity.getExtraFee() + billEntity.getWebFee() != billEntity.getTotalFee())
                        && (billEntity.getTotalFee() != billEntity.getActualFee() + billEntity.getDiscount())) {
                    billTotalFeeError++;
                }
                //todo 其他指标
            }

            List<ErrorMessage> messageList = Lists.newArrayList();

            if (billEndDateBeforStartError > 0
                    || billEndDateEqualStartError > 0
                    || billStartDateNotFirstDayError > 0) {
                //告警统一
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.BILL_DATE_EXTRACT_ERROR), DataMonitorConstants.BILL_DATE_EXTRACT_ERROR);
                //入库细化
                if (billEndDateBeforStartError > 0) {
                    addErrorMsg("billDate", DataMonitorConstants.BILL_ENDDATE_BEFORE_START_ERROR, messageList, billEndDateBeforStartError, billEntityMap.size());
                }
                if (billEndDateEqualStartError > 0) {
                    addErrorMsg("billDate", DataMonitorConstants.BILL_ENDDATE_EQUAL_START_ERROR, messageList, billEndDateEqualStartError, billEntityMap.size());
                }
                if (billStartDateNotFirstDayError > 0) {
                    addErrorMsg("billDate", DataMonitorConstants.BILL_STARTDATE_NOT_FIRSTDAY_ERROR, messageList, billStartDateNotFirstDayError, billEntityMap.size());
                }
            }
            if (billBaseFeeError > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.BILL_BASE_FEE_EXTRACT_ERROR), DataMonitorConstants.BILL_BASE_FEE_EXTRACT_ERROR);
                addErrorMsg("baseFee", DataMonitorConstants.BILL_BASE_FEE_EXTRACT_ERROR, messageList, billBaseFeeError, billEntityMap.size());
            }
            if (billTotalFeeError > 0) {
                incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.BILL_TOTAL_FEE_EXTRACT_ERROR), DataMonitorConstants.BILL_TOTAL_FEE_EXTRACT_ERROR);
                addErrorMsg("totalFee", DataMonitorConstants.BILL_TOTAL_FEE_EXTRACT_ERROR, messageList, billTotalFeeError, billEntityMap.size());
            }
            //入库
            if (messageList.size() > 0) {
                ParserMonitorMessage monitorMessage = copyParserMonitorMessage(billMonitorMessage);
                buildMessage(0, monitorMessage, CodeEnum.CODE_DATA_INVALIDE, messageList);
                addMessageToDb(monitorMessage);
            }
        }
    }

    //无账单数据
    private void handleNoResult(Map<String, List<String>> billCrawlerMap, String keyPre, ParserMonitorMessage billMonitorMessage) {
        incrErrorCount(String.format("%s:%s", keyPre, DataMonitorConstants.BILL_BLANK_RESULT), DataMonitorConstants.BILL_BLANK_RESULT);
        ErrorMessage errorMessage = new ErrorMessage();
        errorMessage.setMessage(DataMonitorConstants.BILL_BLANK_RESULT);
        List<ErrorMessage> errorMessageList = Lists.newArrayList();
        errorMessageList.add(errorMessage);
        buildMessage(billCrawlerMap.size(), billMonitorMessage, CodeEnum.CODE_NO_DATA, errorMessageList);
        //入库
        addMessageToDb(billMonitorMessage);
    }

    @Override
    public String getType() {
        return DataTypeEnum.BILL.name();
    }
}
