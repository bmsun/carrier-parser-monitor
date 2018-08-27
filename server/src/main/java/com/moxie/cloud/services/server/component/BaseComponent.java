package com.moxie.cloud.services.server.component;

import com.moxie.cloud.carrier.entity.CarrierPropertiesEntity;
import com.moxie.cloud.service.common.dto.MoxieApiErrorMessage;
import com.moxie.cloud.service.common.exception.MoxieApiException;
import com.moxie.cloud.services.common.MonitorServiceConstants;
import com.moxie.cloud.services.common.dto.CarrierVerifty;
import com.moxie.cloud.services.common.dto.MobileDetailInfo;
import com.moxie.cloud.services.common.dto.taskarchive.ErrorMessage;
import com.moxie.cloud.services.common.dto.taskarchive.ParserMonitorMessage;
import com.moxie.cloud.services.common.enums.CodeEnum;
import com.moxie.cloud.services.common.metadata.CrawlerMetadataModel;
import com.moxie.cloud.services.common.metadata.MedataResultModel;
import com.moxie.cloud.services.common.metadata.MetadateModel;
import com.moxie.cloud.services.server.cache.CarrierPropertiesCache;
import com.moxie.cloud.services.server.config.AppProperties;
import com.moxie.cloud.services.server.service.MonitorMessageService;
import com.moxie.cloud.services.server.service.MonitorService;
import com.moxie.cloud.services.server.service.RedisService;
import com.moxie.commons.MoxieBeanUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.collections.Maps;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.math.BigDecimal.ROUND_HALF_UP;

@Slf4j
public abstract class BaseComponent {
    protected final static Pattern IS_CARD = Pattern.compile("\\d+[Xx]{0,1}");
    protected final static Pattern IS_NUMBER = Pattern.compile("^[\\d|+|-|(|)]+$");
    protected final static Pattern IS_CHINESE_REGEX = Pattern.compile("[\\u4E00-\\u9FBF]+");
    protected final static String MX_ONE = "MX_ONE";
    protected final static String CRAWL_FAIL_CODE = "CACR-21301-30";
    @Autowired
    RedisService redisService;

    @Autowired
    MonitorService monitorService;

    @Autowired
    AppProperties appProperties;

    @Autowired
    CarrierPropertiesCache propertiesCache;

    @Autowired
    MonitorMessageService monitorMessageService;


    public abstract void verifyData(CrawlerMetadataModel crawlerMetadataModel, MobileDetailInfo mobileDetailInfo, CarrierVerifty carrierVerify, String keyPre, ParserMonitorMessage monitorMessage);

    public abstract String getType();

    protected boolean isTrue(String flag) {
        if ("Y".equalsIgnoreCase(flag)) return true;
        return false;
    }

    protected void sendErrorMsg(String errorMsg, BigDecimal useTime, Long count, int maxErrorCount) {
        String[] webhookTokens = appProperties.getWebhookToken().split(",");
        try {
            monitorService.sendDingRobot(webhookTokens, errorMsg, useTime, count, maxErrorCount);
        } catch (Exception e) {
            log.error("msgSendServiceClient DingRobotRemind msg:[{}] occur error", errorMsg);
        }
    }

    /**
     * error +1
     */
    protected void incrErrorCount(String key, String field) {
        int maxErrorCount = getMaxErrorCount();
        int expireTime = getExpireTime();
        //加锁
        String requestId = UUID.randomUUID().toString();
        //todo 分布锁，暂定过期时间3秒
        try {
            if (redisService.tryGetDistributedLock(key + MonitorServiceConstants.REDIS_LOCK, requestId, 3)) {
                if (redisService.exists(key)) {
                    Long count = redisService.hIncrby(key, field, 1);
                    if (count >= maxErrorCount) {
                        Long ttl = redisService.ttl(key);
                        BigDecimal useTime;
                        if (ttl != null && ttl > 0) {
                            useTime = BigDecimal.valueOf(expireTime - ttl).divide(new BigDecimal(60), ROUND_HALF_UP);
                        } else {
                            useTime = BigDecimal.valueOf(expireTime).divide(new BigDecimal(60), ROUND_HALF_UP);
                        }
                        /**
                         * 超过阀值 发送钉钉告警
                         * */
                        this.sendErrorMsg(key, useTime, count, maxErrorCount);
                    }
                } else {
                    redisService.init(key, field, expireTime);
                }
            }
        } finally {
            //释放锁
            redisService.releaseDistributedLock(key + MonitorServiceConstants.REDIS_LOCK, requestId);
        }


    }


    protected int addMessageToDb(ParserMonitorMessage monitorMessage) {
        return monitorMessageService.addParserMonitorMessage(monitorMessage);
    }

    /**
     * 获取告警阀值
     */
    private int getMaxErrorCount() {
        CarrierPropertiesEntity carrierProperties = propertiesCache.getCarrierProperties(MonitorServiceConstants.MAX_ERROR_COUNT);
        int maxErrorCount = MonitorServiceConstants.MAX_ERROR_COUNT_NUM;
        if (carrierProperties != null && StringUtils.isNotBlank(carrierProperties.getPropertyValue())) {
            try {
                maxErrorCount = Integer.parseInt(carrierProperties.getPropertyValue());
            } catch (NumberFormatException e) {
                maxErrorCount = MonitorServiceConstants.MAX_ERROR_COUNT_NUM;
            }
        }
        return maxErrorCount;
    }


    /**
     * 获取Key有效期时间：即统计错误码的单位时间
     */
    private int getExpireTime() {
        CarrierPropertiesEntity carrierProperties = propertiesCache.getCarrierProperties(MonitorServiceConstants.MONITOR_TIME);
        int expireTime = MonitorServiceConstants.REDIS_EXPIRE_TIME;
        if (carrierProperties != null && StringUtils.isNotBlank(carrierProperties.getPropertyValue())) {
            try {
                expireTime = Integer.parseInt(carrierProperties.getPropertyValue());
            } catch (NumberFormatException e) {
                expireTime = MonitorServiceConstants.REDIS_EXPIRE_TIME;
            }
        }
        return expireTime;
    }

    //格式化 yyyyMM
    protected String formatMonth(Date date) {
        return date == null ? "" : DateFormatUtils.format(date, "yyyyMM");
    }


    protected ParserMonitorMessage copyParserMonitorMessage(ParserMonitorMessage monitorMessage) {
        ParserMonitorMessage message = null;
        try {
            message = (ParserMonitorMessage) BeanUtils.cloneBean(monitorMessage);
        } catch (Exception e) {
            throw new MoxieApiException(new MoxieApiErrorMessage(" clone Bean ParserMonitorMessage occur error", 500, e.getMessage()));
        }
        return message;
    }

    protected void buildMessage(int count, ParserMonitorMessage message, CodeEnum codeEnum, List<ErrorMessage> errorMessageList) {
        //缺失月份
        message.setCount(count);
        message.setCode(codeEnum.getCode());
        message.setErrorMsg(MoxieBeanUtils.getJsonString(errorMessageList));
    }

    //添加异常信息
    protected void addErrorMsg(String type, String value, List<ErrorMessage> msgList, Integer errorNum, Integer totalNum) {
        ErrorMessage errorMessage = new ErrorMessage();
        errorMessage.setType(type);
        errorMessage.setMessage(value);
        errorMessage.setErrorNum(errorNum);
        errorMessage.setTotalNum(totalNum);
        if (totalNum != 0) {
            double rate = Double.valueOf(errorNum) / Double.valueOf(totalNum);
            BigDecimal bg = new BigDecimal(rate).multiply(new BigDecimal(100));
            BigDecimal scale = bg.setScale(2, RoundingMode.HALF_UP);
            errorMessage.setRate(scale + "%");
        }
        msgList.add(errorMessage);
    }

    /**
     * 获取本次任务crawl metadata信息
     */
    protected Map<String, List<String>> getCrawlerMap(CrawlerMetadataModel crawlerMetadataModel) {
        List<MetadateModel> crawlers = crawlerMetadataModel.getCrawler();
        if (crawlers == null || crawlers.isEmpty()) return null;
        //本次任务实际爬取月份与errorcode
        Map<String, List<String>> crawlerMap = Maps.newHashMap();
        for (MetadateModel model : crawlers) {
            List<String> errorCodes = model.getErrors().stream().map(MedataResultModel::getErrorCode).collect(Collectors.toList());
            if (model.getBillDate() != null) {
                crawlerMap.put(model.getBillDate(), errorCodes);
            } else {
                //针对广东移动网厅详单在同一个文件中 或shop/cmcc 账单在同一个文件中
                crawlerMap.put(MX_ONE, errorCodes);
            }
        }
        return crawlerMap;
    }

    protected Pair<Integer, Integer> checkData(Map<String, List<String>> crawlerMap, Map<String, Long> groupMap) {
        int crawlErrorNum = 0;
        int parserErrorNum = 0;
        for (Map.Entry<String, List<String>> entry : crawlerMap.entrySet()) {
            List<String> value = entry.getValue();
            if (groupMap.get(entry.getKey()) == null
                    && value.size() == 1
                    //采集失败 CACR-21301-30
                    && value.get(0).equals(CRAWL_FAIL_CODE)) {
                crawlErrorNum++;
            } else if (//排除一个文件包含多个月份数据的情况
                    !entry.getKey().equals(MX_ONE)
                            && groupMap.get(entry.getKey()) == null) {
                //当月无记录也计作异常
                parserErrorNum++;
            }
        }
        return Pair.of(crawlErrorNum,parserErrorNum);
    }


}
