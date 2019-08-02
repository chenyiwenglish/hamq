package com.chenyiwenglish.hamq;

public interface MessageQueueConsumer {
    void registerProcessor(MessageProcessor messageProcessor);
}
