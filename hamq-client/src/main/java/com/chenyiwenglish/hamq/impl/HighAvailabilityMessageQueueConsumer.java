package com.chenyiwenglish.hamq.impl;

import com.alibaba.fastjson.JSON;
import com.chenyiwenglish.hamq.MessageProcessor;
import com.chenyiwenglish.hamq.MessageQueueConsumer;
import com.chenyiwenglish.hamq.MessageQueueException;
import com.chenyiwenglish.hamq.enumeration.Constants;
import com.chenyiwenglish.hamq.enumeration.FadingType;
import com.chenyiwenglish.hamq.enumeration.Status;
import com.chenyiwenglish.hamq.mapper.MessageQueueInfoMapper;
import com.chenyiwenglish.hamq.mapper.MessageQueueMapper;
import com.chenyiwenglish.hamq.model.LogInfo;
import com.chenyiwenglish.hamq.model.Message;
import com.chenyiwenglish.hamq.model.MessageQueueInfo;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Data
public class HighAvailabilityMessageQueueConsumer implements MessageQueueConsumer {

    private int workPoolCoreSize = Constants.WORK_POOL_CORE_SIZE;
    private int workPoolMaxSize = Constants.WORK_POOL_MAX_SIZE;

    private MessageQueueInfo configuration;

    private MessageQueueMapper messageQueueMapper;

    private MessageQueueInfoMapper messageQueueInfoMapper;

    private String idc;

    private volatile int status;
    private volatile boolean registered;
    private volatile boolean running;

    private MessageProcessor messageProcessor;
    private ThreadPoolExecutor workThreadPool;
    private Thread consumeThread;
    private ScheduledExecutorService checkQueueStatusExecutor;

    public void init() {
        long ts = System.currentTimeMillis();
        log.info("HAMQC begin to init, queueId:{}", configuration.getQueueId());

        MessageQueueInfo messageQueueInfo = messageQueueInfoMapper.selectByQueueId(configuration.getQueueId());
        if (messageQueueInfo == null) {
            log.error("HAMQC no record in mysql, queueId:{}", configuration.getQueueId());
            throw new MessageQueueException("cannot find message queue in db, queueId:" + configuration.getQueueId());
        } else if (messageQueueInfo.getFadingType() < FadingType.NON.getId()
                || messageQueueInfo.getFadingType() > FadingType.EXPONENTIAL.getId()) {
            log.error("HAMQC illegal fadingType, queueId:{}, fadingType:{}", configuration.getQueueId(),
                    messageQueueInfo.getFadingType());
            throw new MessageQueueException("illegal message queue fadingType, queueId:" + configuration.getQueueId()
                    + "fadingType:" + messageQueueInfo.getFadingType());
        } else if (messageQueueInfo.getMaxRetryCount() < 0) {
            log.error("HAMQC illegal maxRetryCount, queueId:{}, maxRetryCount:{}", configuration.getQueueId(),
                    messageQueueInfo.getMaxRetryCount());
            throw new MessageQueueException("illegal message queue maxRetryCount, queueId:" + configuration.getQueueId()
                    + "maxRetryCount:" + messageQueueInfo.getMaxRetryCount());
        }
        configuration.setMaxRetryCount(messageQueueInfo.getMaxRetryCount());
        configuration.setFadingType(messageQueueInfo.getFadingType());

        status = messageQueueInfo.getStatus();
        running = false;
        registered = false;

        log.info("finish init cost:{}, queueId:{}", (System.currentTimeMillis() - ts), configuration.getQueueId());
    }

    public void destroy() {
        if (registered) {
            running = false;
            consumeThread.interrupt();
            workThreadPool.shutdown();
            checkQueueStatusExecutor.shutdown();
            try {
                consumeThread.join(Constants.SHUTDOWN_MAX_WAIT_TIME);
                workThreadPool.awaitTermination(Constants.SHUTDOWN_MAX_WAIT_TIME, TimeUnit.MILLISECONDS);
                checkQueueStatusExecutor.awaitTermination(Constants.SHUTDOWN_MAX_WAIT_TIME, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // do nothing
            }
        }
    }

    @Override
    public void registerProcessor(MessageProcessor messageProcessor) {
        long ts = System.currentTimeMillis();
        log.info("begin to registerProcessor, queueId:{}", configuration.getQueueId());
        if (messageProcessor == null) {
            log.error("messageProcessor is null, queueId:{}", configuration.getQueueId());
            throw new MessageQueueException("messageProcessor is null, queueId:" + configuration.getQueueId());
        }

        this.messageProcessor = messageProcessor;

        workThreadPool = new ThreadPoolExecutor(
                workPoolCoreSize,
                workPoolMaxSize,
                Constants.WORK_POOL_IDLE_TIME,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        consumeThread = new Thread(new ConsumeRunnable());
        consumeThread.start();
        running = true;

        checkQueueStatusExecutor = Executors.newSingleThreadScheduledExecutor();
        checkQueueStatusExecutor.scheduleAtFixedRate(
                new CheckQueueStatusRunnable(),
                Constants.HAMQ_STATUS_CHECK_INTERVAL,
                Constants.HAMQ_STATUS_CHECK_INTERVAL,
                TimeUnit.MILLISECONDS
        );

        registered = true;

        log.info("finish registerProcessor, cost:{}, queueId:{}", (System.currentTimeMillis() - ts),
                configuration.getQueueId());
    }

    private class CheckQueueStatusRunnable implements Runnable {

        @Override
        public void run() {
            try {
                MessageQueueInfo messageQueueInfo = messageQueueInfoMapper.selectByQueueId(configuration.getQueueId());
                if (messageQueueInfo == null) {
                    throw new MessageQueueException(
                            "cannot find message queue in db, queueId:" + configuration.getQueueId());
                }
                status = messageQueueInfo.getStatus();
            } catch (Throwable e) {
                log.error("check status failed, queueId:{}", configuration.getQueueId());
            }
        }
    }

    public class ConsumeRunnable implements Runnable {

        @Override
        public void run() {
            while (running) {
                try {
                    while (running) {
                        if (status == Status.PAUSE.getId() || !running) {
                            // 队列已经被暂停 或 整个jvm准备退出
                            break;
                        }

                        log.debug("try to get a message from mysql, queueId:{}", configuration.getQueueId());
                        Message messageBean =
                                messageQueueMapper.selectByQueueIdAndIdcAndConsumeTimeOldest(configuration.getQueueId(),
                                        idc, System.currentTimeMillis());
                        if (messageBean == null) {
                            break;
                        }
                        log.debug("HAMQ maybe get a message from mysql, queueId:{}", configuration.getQueueId());
                        // 加锁, 防止其他实例取到该消息
                        int count = messageQueueMapper.lockByMessageId(messageBean.getMessageId());
                        if (count == 0) {
                            // 已经被其他线程取到, 不再继续
                            continue;
                        }

                        log.info("gotten a message from mysql, queueId:{}, messageId:{}, messageBody:{}",
                                configuration.getQueueId(), messageBean.getMessageId(), messageBean.getMessageBody());
                        workThreadPool.submit(new TaskRunnable(messageBean));
                    }

                    if (!running) {
                        break;
                    }

                    try {
                        Thread.sleep(Constants.HAMQ_MYSQL_CHECK_INTERVAL);
                    } catch (InterruptedException e) {
                        break;
                    }
                } catch (Throwable e) {
                    if (!running) {
                        break;
                    }
                    log.error("unexpected exception, queueId:{}, exception:{}", configuration.getQueueId(),
                            e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    public class TaskRunnable implements Runnable {
        private Message messageBean;

        public TaskRunnable(Message messageBean) {
            this.messageBean = messageBean;
        }

        @Override
        public void run() {
            boolean isProcessSuccessful = false;
            long tp = System.currentTimeMillis();
            try {
                LogInfo logInfo = new LogInfo();
                try {
                    logInfo = JSON.parseObject(messageBean.getExtraInfo(), LogInfo.class);
                } catch (Throwable e) {
                    log.error("decode extraInfo exception queueId:{}, extraInfo:{}, exception:{}",
                            configuration.getQueueId(), messageBean.getExtraInfo(), e.getMessage());
                    e.printStackTrace();
                }

                String logId = logInfo.getReqId();
                MDC.put("reqId", logId != null ? logId : "");
                log.debug("begin to process, queueId:{}, messageId:{}, messageBody:{}",
                        configuration.getQueueId(), messageBean.getMessageId(), messageBean.getMessageBody());
                messageProcessor.process(messageBean.getMessageBody());
                isProcessSuccessful = true;
            } catch (Throwable e) {
                log.warn("consume exception:{}", e.getMessage());
                e.printStackTrace();
            }

            if (isProcessSuccessful) {
                log.info("process successful, cost:{}, queueId:{}, messageId:{}, messageBody:{}",
                        (System.currentTimeMillis() - tp), configuration.getQueueId(), messageBean.getMessageId(),
                        messageBean.getMessageBody());
                deleteMessage();
            } else {
                log.info("process failed, cost:{}, queueId:{}, messageId:{}, messageBody:{}",
                        (System.currentTimeMillis() - tp), configuration.getQueueId(), messageBean.getMessageId(),
                        messageBean.getMessageBody());
                if (dontRetry(messageBean.getRetryCount())) {
                    // 达到最大重试次数, 不需要再重试了
                    log.info("retry all failed, cost:{}, queueId:{}, messageId:{}, messageBody:{}",
                            (System.currentTimeMillis() - tp), configuration.getQueueId(), messageBean.getMessageId(),
                            messageBean.getMessageBody());
                    deleteMessage();
                    return;
                }
                int retryCount = messageBean.getRetryCount();
                messageBean.setRetryCount(retryCount + 1);
                messageBean.setConsumeTime(getNextConsumeTime(retryCount));

                int tmpRetryCount = 0;
                while (true) {
                    try {
                        messageQueueMapper.insert(messageBean, idc);
                        break;
                    } catch (Throwable e) {
                        tmpRetryCount++;
                        if (tmpRetryCount > 1) {
                            log.error("push mysql failed, queueId:{}, messageId:{}, messageBody:{}, exception:{}",
                                    configuration.getQueueId(), messageBean.getMessageId(),
                                    messageBean.getMessageBody(), e.getMessage());
                            e.printStackTrace();
                            break;
                        }
                    }
                }
            }
        }

        private void deleteMessage() {
            int retryCount = 0;
            while (true) {
                try {
                    messageQueueMapper
                            .deleteByMessageIdAndQueueId(messageBean.getMessageId(), configuration.getQueueId());
                    break;
                } catch (Throwable e) {
                    retryCount++;
                    if (retryCount > 1) {
                        log.error("message delete failed, queueId:{}, messageId:{}, messageBody:{}, exception:{}",
                                configuration.getQueueId(), messageBean.getMessageId(), messageBean.getMessageBody(),
                                e.getMessage());
                        e.printStackTrace();
                        break;
                    }
                }
            }
        }

        private boolean dontRetry(int retryCount) {
            return (configuration.getMaxRetryCount() > 0 && retryCount > configuration.getMaxRetryCount());
        }

        private long getNextConsumeTime(int retryCount) {
            long now = System.currentTimeMillis();
            int delta = 0;
            if (configuration.getFadingType() == FadingType.LINEAR.getId()) {
                delta = retryCount * 100;
            } else if (configuration.getFadingType() == FadingType.SQUARE.getId()) {
                delta = retryCount * retryCount * 100;
            } else if (configuration.getFadingType() == FadingType.CUBIC.getId()) {
                delta = retryCount * retryCount * retryCount * 100;
            } else if (configuration.getFadingType() == FadingType.EXPONENTIAL.getId()) {
                delta = (int) Math.pow(Math.E, retryCount) * 100;
            }

            if (delta > Constants.FADING_MAX_TIME) {
                delta = Constants.FADING_MAX_TIME;
            }

            return now + delta;
        }
    }
}
