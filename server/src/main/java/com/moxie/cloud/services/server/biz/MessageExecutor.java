package com.moxie.cloud.services.server.biz;


import com.moxie.cloud.services.common.enums.TaskTypeEnum;

public interface MessageExecutor {


    TaskTypeEnum getTaskType();

    void execute(Object message) throws Exception;
}
