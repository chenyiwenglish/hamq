package com.chenyiwenglish.hamq;

public interface MessageQueueProducer {
    boolean produce(String messageBody);

    boolean produce(String messageBody, long consumeTime);
}
