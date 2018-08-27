package com.moxie.cloud.services.server.factory;

import com.moxie.cloud.services.common.dto.ParserMonitorInfo;
import com.moxie.cloud.services.common.enums.TaskTypeEnum;
import com.moxie.cloud.services.server.biz.MessageExecutor;
import com.moxie.cloud.services.server.biz.impl.MonitorMesageExcutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class MessageExecutorFactory {

    private Map<TaskTypeEnum, MessageExecutor> messageExecutorMap = new HashMap<>();

    @Autowired
    private MonitorMesageExcutor monitorMesageExcutor;

    /**
     * 初始化消息处理器
     */
    @PostConstruct
    public void init() {
        messageExecutorMap.put(monitorMesageExcutor.getTaskType(), monitorMesageExcutor);
    }

    /**
     * 获取消息处理器
     * @param taskType
     * @return
     */
    public MessageExecutor getMessageExecutor(String taskType) {
        MessageExecutor messageExecutor = null;
        TaskTypeEnum taskTypeEnum = TaskTypeEnum.getByType(taskType);
        if (taskTypeEnum != null) {
            messageExecutor = messageExecutorMap.get(taskTypeEnum);
        }
        return messageExecutor;
    }

    /**
     * 处理任务
     * @param parserMonitorInfo
     */
    public void process(ParserMonitorInfo parserMonitorInfo) {

        if (parserMonitorInfo == null) {
            log.error("action=process, process message but get message is null");
            return;
        }
        MessageExecutor messageExecutor = getMessageExecutor(monitorMesageExcutor.getTaskType().getType());
        if (messageExecutor == null) {
            log.error("action=process, task_type:{}, process message but can not find a message executor to handler it.",  monitorMesageExcutor.getTaskType().name());
            return;
        }
        try {
            messageExecutor.execute(parserMonitorInfo);
        } catch (Exception e) {
           log.error("action=process, task_type:{}, process message occur exception", monitorMesageExcutor.getTaskType().name(), e);
        }
    }
}
