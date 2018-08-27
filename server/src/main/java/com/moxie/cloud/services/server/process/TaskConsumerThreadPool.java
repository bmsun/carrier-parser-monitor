package com.moxie.cloud.services.server.process;


import com.moxie.cloud.services.common.dto.ParserMonitorInfo;
import com.moxie.cloud.services.server.config.AppProperties;
import com.moxie.cloud.services.server.factory.MessageExecutorFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


@Component
@Slf4j
public class TaskConsumerThreadPool {

    private LinkedBlockingQueue<ParserMonitorInfo> processQueue = new LinkedBlockingQueue<>();

    /**
     * 线程池中正在运行的任务数
     */
    private static AtomicInteger count = new AtomicInteger(0);

    private ExecutorService executor;

    private boolean running = true;

    @Autowired
    private AppProperties appProperties;

    @Autowired
    private MessageExecutorFactory messageExecutorFactory;

    @PostConstruct
    public void init() {
        int threadSize = appProperties.getCorePoolSize();
        long noTaskSleepMilliSec = appProperties.getNoTaskSleepMilliSec();
        this.executor = Executors.newFixedThreadPool(threadSize);

        for (int i =0; i < threadSize; ++i) {
            this.executor.execute(() -> {
                while(this.running) {
                    ParserMonitorInfo parserMonitorInfo = null;
                    try {
                        parserMonitorInfo = this.processQueue.poll(5L, TimeUnit.SECONDS);
                        if (parserMonitorInfo != null) {
                            log.info("action=getMessageFromQueue, current queue size is:{}", processQueue.size());
                            increment();
                            // 开始做业务
                            messageExecutorFactory.process(parserMonitorInfo);
                            decrement();
                        } else {
                            log.debug("action=getMessageFromQueue, get no task from process queue, prepare to sleep {} ms", noTaskSleepMilliSec);
                            try {
                                TimeUnit.MILLISECONDS.sleep(noTaskSleepMilliSec);
                            } catch (InterruptedException e) {
                               log.error("action=getMessageFromQueue, no task sleep occur exception", e);
                            }
                        }
                    } catch (Throwable e) {
                        if (parserMonitorInfo!=null) {
                            log.error("action=threadRun, key:{}, 执行任务异常", e);
                            decrement();
                        }
                    }
                }
                log.info("action=threadRun, 线程结束任务");
            });
        }
    }

    /**
     * 添加任务
     * @param parserMonitorInfo
     */
    public void addTask(ParserMonitorInfo parserMonitorInfo) {
        processQueue.add(parserMonitorInfo);
    }

    /**
     * 批量添加任务
     * @param parserMonitorInfoList
     */
    public void batchAddTask(List<ParserMonitorInfo> parserMonitorInfoList) {
        processQueue.addAll(parserMonitorInfoList);
    }

    private static int increment() {
        return count.incrementAndGet();
    }

    private static int decrement() {
        return count.decrementAndGet();
    }

    /**
     * 获取正在做的任务数
     * @return
     */
    public int getSize() {
        return count.get();
    }

    /**
     * 获取等待在队列里的任务数
     * @return
     */
    public int getWaitingQueueSize() {
        return this.processQueue.size();
    }

    /**
     * 应用关闭，等待线程池中的任务结束
     * 最多等待1分钟
     */
    @PreDestroy
    public void destroy(){
        this.running = false;
        log.info("action=destroy, application start to shutdown , wait thread pool to done all the job");
        try {
            executor.shutdown();
            executor.awaitTermination(3, TimeUnit.MINUTES);
        } catch (Exception var2) {
            log.error("action=destroy, thread pool not shutdown completed, occur exception", var2);
        }
    }


}
