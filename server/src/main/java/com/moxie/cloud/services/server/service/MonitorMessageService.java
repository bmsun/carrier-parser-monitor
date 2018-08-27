package com.moxie.cloud.services.server.service;


import com.moxie.cloud.service.common.dto.MoxieApiErrorMessage;
import com.moxie.cloud.service.common.exception.MoxieApiException;
import com.moxie.cloud.services.common.dao.ParserMonitorMessageDao;
import com.moxie.cloud.services.common.dto.taskarchive.ParserMonitorMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;

@Slf4j
@Service
public class MonitorMessageService {

    @Autowired
    ParserMonitorMessageDao parserMonitorMessageDao;

    public int addParserMonitorMessage(ParserMonitorMessage monitorMessage) throws MoxieApiException {
        if (monitorMessage == null) {
            log.error("action=addParserMonitorMessage, param: monitorMessage is null");
            return 0;
        }
        try {
            List<ParserMonitorMessage> message = parserMonitorMessageDao.getMonitorMessage(monitorMessage.getCode(), monitorMessage.getTaskId(), monitorMessage.getType());
            if (CollectionUtils.isEmpty(message)){
                monitorMessage.setCreateTime(new Date());
                return parserMonitorMessageDao.addParserMonitorMessage(monitorMessage);
            }else {
                //根据taskid,code,type 更新
                monitorMessage.setUpdateTime(new Date());
                return parserMonitorMessageDao.updateMonitorMessage(monitorMessage);
            }
        } catch (Exception e) {
            log.error("action=addParserMonitorMessage,task_id:{}", monitorMessage.getTaskId(), e);
            return 0;

        }
    }
}
