package com.chenyiwenglish.hamq.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.chenyiwenglish.hamq.mapper.MessageQueueInfoMapper;
import com.chenyiwenglish.hamq.mapper.MessageQueueMapper;
import com.chenyiwenglish.hamq.model.LogInfo;
import com.chenyiwenglish.hamq.model.Message;
import com.chenyiwenglish.hamq.model.MessageQueueInfo;
import org.slf4j.MDC;

import com.alibaba.fastjson.JSON;
import com.chenyiwenglish.hamq.MessageIdGenerator;
import com.chenyiwenglish.hamq.MessageQueueException;
import com.chenyiwenglish.hamq.MessageQueueProducer;
import com.chenyiwenglish.hamq.enumeration.Constants;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class HighAvailabilityMessageQueueProducer implements MessageQueueProducer {

    private MessageQueueInfo configuration;

    private MessageQueueInfoMapper messageQueueInfoMapper;

    private MessageQueueMapper messageQueueMapper;

    private MessageIdGenerator messageIdGenerator;

    private String idc;

    private Path dataDirectory;

    private ScheduledExecutorService checkDataFileExecutor;

    public void init() throws IOException {
        long ts = System.currentTimeMillis();
        log.info("begin to init, queueId:{}", configuration.getQueueId());

        MessageQueueInfo messageQueueInfo = messageQueueInfoMapper.selectByQueueId(configuration.getQueueId());
        if (messageQueueInfo == null) {
            log.error("no record in mysql, queueId:{}", configuration.getQueueId());
            throw new MessageQueueException("cannot find message queue in db, queueId:" + configuration.getQueueId());
        }

        Path tmpDataDir = Paths.get(Constants.HAMQ_DATA_FILE_PATH_PREFIX, String.valueOf(configuration.getQueueId()));
        dataDirectory = Files.createDirectories(tmpDataDir);

        checkDataFileExecutor = Executors.newSingleThreadScheduledExecutor();
        checkDataFileExecutor.scheduleAtFixedRate(
                new CheckDataFileRunnable(),
                Constants.HAMQ_DATA_FILE_CHECK_INTERVAL,
                Constants.HAMQ_DATA_FILE_CHECK_INTERVAL,
                TimeUnit.MILLISECONDS
        );

        log.info("finish init cost:{}, queueId:{}", (System.currentTimeMillis() - ts), configuration.getQueueId());
    }

    public void destroy() {
        checkDataFileExecutor.shutdown();
        try {
            checkDataFileExecutor.awaitTermination(Constants.SHUTDOWN_MAX_WAIT_TIME, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // do nothing
        }
    }

    @Override
    public boolean produce(String messageBody) {
        return produce(messageBody, System.currentTimeMillis());
    }

    @Override
    public boolean produce(String messageBody, long consumeTime) {
        log.info("begin to produce, queueId:{}, consumeTime:{}, messageBody:{}",
                configuration.getQueueId(), consumeTime, messageBody);

        String reqId = MDC.get("reqId");
        LogInfo logInfo = new LogInfo();
        logInfo.setReqId(reqId != null ? reqId : "");
        String extraInfo = "";
        try {
            extraInfo = JSON.toJSONString(logInfo);
        } catch (Throwable e) {
            log.error("encode extraInfo exception, queueId:{}, exception:{}", configuration.getQueueId(),
                    e.getMessage());
            e.printStackTrace();
        }

        Message messageBean = new Message();
        messageBean.setExtraInfo(extraInfo);
        messageBean.setQueueId(configuration.getQueueId());
        messageBean.setRetryCount(0);
        messageBean.setMessageBody(messageBody);
        messageBean.setConsumeTime(consumeTime);

        Long messageId = getMessageId();
        if (messageId == null) {
            return writeFile(messageBean);
        }
        messageBean.setMessageId(messageId);

        int retryCount = 0;
        long tp = System.currentTimeMillis();
        while (true) {
            try {
                log.debug("begin to push mysql, queueId:{}, messageId:{}, messageBody:{}",
                        configuration.getQueueId(), messageId, messageBody);
                messageQueueMapper.insert(messageBean, idc);
                log.debug("push mysql successful, cost:{}, queueId:{}, messageId:{}, messageBody:{}",
                        (System.currentTimeMillis() - tp), configuration.getQueueId(), messageId, messageBody);
                return true;
            } catch (Throwable e) {
                retryCount++;
                if (retryCount > 2) {
                    log.error("push mysql failed, queueId:{}, messageId:{}, messageBody:{}, exception:{}",
                            configuration.getQueueId(), messageId, messageBody, e.getMessage());
                    e.printStackTrace();
                    break;
                }
            }
        }

        return writeFile(messageBean);
    }

    private Long getMessageId() {
        return messageIdGenerator.generateId();
    }

    private boolean writeFile(Message messageBean) {
        log.info("begin to writeFile, queueId:{}", configuration.getQueueId());

        String messageString = "";
        try {
            messageString = JSON.toJSONString(messageBean) + "\n";
        } catch (Throwable e) {
            log.error("encode messageBean exception, queueId:{}, messageId:{}, messageBody:{}, exception:{}",
                    configuration.getQueueId(), messageBean.getMessageId(), messageBean.getMessageBody(),
                    e.getMessage());
            e.printStackTrace();
            return false;
        }

        String currDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String currDataFileName = currDate + ".ready";
        Path currDataFilePath = null;
        try {
            currDataFilePath = dataDirectory.resolve(currDataFileName);
        } catch (Throwable e) {
            log.error("resolve data file path exception, queueId:{}, dataDirectory:{}, filename:{}, messageId:{}, "
                            + "messageBody:{}, exception:{}", configuration.getQueueId(), dataDirectory.toString(),
                    currDataFileName,
                    messageBean.getMessageId(), messageBean.getMessageBody(), e.getMessage());
            e.printStackTrace();
            return false;
        }
        int retryCount = 0;
        while (true) {
            try {
                BufferedWriter bw = Files.newBufferedWriter(
                        currDataFilePath,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND
                );
                bw.write(messageString);
                bw.close();
                break;
            } catch (Throwable e) {
                retryCount += 1;
                if (retryCount >= 3) {
                    log.error(
                            "write data file exception, queueId:{}, filename:{}, messageId:{}, messageBody:{}, "
                                    + "exception:{}",
                            configuration.getQueueId(), currDataFileName, messageBean.getMessageId(),
                            messageBean.getMessageBody(),
                            e.getMessage());
                    e.printStackTrace();
                    return false;
                }
            }
        }
        return true;
    }

    public class CheckDataFileRunnable implements Runnable {
        @Override
        public void run() {
            try {
                TreeSet<String> readyFilenames = new TreeSet<>(
                        Arrays.asList(
                                dataDirectory.toFile().list(
                                        new EndsWithFilter(Constants.HAMQ_DATA_FILE_READY_SUFFIX)
                                )
                        )
                );

                for (String readyFilename : readyFilenames) {
                    Path readyFile = dataDirectory.resolve(readyFilename);
                    int pos = readyFilename.lastIndexOf(Constants.HAMQ_DATA_FILE_READY_SUFFIX);
                    if (pos == -1) {
                        log.error("not find ready data file suffix, queueId:{}, filename:{}",
                                configuration.getQueueId(), readyFilename);
                        return;
                    }
                    String successFilename = readyFilename.substring(0, pos) + Constants.HAMQ_DATA_FILE_SUCCESS_SUFFIX;
                    Path successFile = dataDirectory.resolve(successFilename);
                    int successLineCount = 0;
                    try {
                        BufferedReader successReader = Files.newBufferedReader(successFile, StandardCharsets.UTF_8);
                        String line = successReader.readLine();
                        while (line != null) {
                            successLineCount += 1;
                            line = successReader.readLine();
                        }
                    } catch (NoSuchFileException e) {
                        // do nothing
                    } catch (IOException e) {
                        log.error("read success data file exception, queueId:{}, filename:{}, exception:{}",
                                configuration.getQueueId(), successFilename, e.getMessage());
                    }

                    try (BufferedReader readyReader = Files.newBufferedReader(readyFile, StandardCharsets.UTF_8);
                         BufferedWriter successWriter = Files.newBufferedWriter(successFile, StandardCharsets.UTF_8,
                                 StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                        String messageString = null;
                        int lineNumber = 0;
                        while ((messageString = readyReader.readLine()) != null) {
                            lineNumber += 1;
                            if (lineNumber <= successLineCount) {
                                continue;
                            }
                            Message messageBean = JSON.parseObject(messageString, Message.class);
                            if (messageBean.getMessageId() == null) {
                                Long messageId = getMessageId();
                                if (messageId == null) {
                                    log.error("get message id failed, queueId:{}, filename:{}",
                                            configuration.getQueueId(), readyFilename);
                                    return;
                                }
                                messageBean.setMessageId(messageId);
                            }
                            messageQueueMapper.insert(messageBean, idc);
                            successWriter.write(
                                    LocalDateTime.now().format(
                                            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
                                    )
                            );
                            successWriter.newLine();
                            successWriter.flush();
                        }
                        successWriter.close();

                        BasicFileAttributes bfa = Files.readAttributes(readyFile, BasicFileAttributes.class);
                        long lastModifiedTime = bfa.lastModifiedTime().toMillis();
                        if (System.currentTimeMillis() - lastModifiedTime > Constants.HAMQ_DATA_FILE_RESERVE_MAX_TIME) {
                            Files.deleteIfExists(readyFile);
                            Files.deleteIfExists(successFile);
                        }
                    } catch (IOException e) {
                        log.error("process ready/success data file exception, queueId:{}, filename:{}, exception:{}",
                                configuration.getQueueId(), readyFilename, e.getMessage());
                        e.printStackTrace();
                    }
                }
            } catch (Throwable e) {
                log.error("unexpected exception, queueId:{}, exception:{}", configuration.getQueueId(), e.getMessage());
                e.printStackTrace();
            }
        }

        private class EndsWithFilter implements FilenameFilter {
            private String suffix;

            EndsWithFilter(String suffix) {
                this.suffix = suffix;
            }

            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(suffix);
            }
        }
    }
}
