package com.chenyiwenglish.hamq;

public class MessageQueueException extends RuntimeException {
    public MessageQueueException(String message) {
        super(message);
    }

    public MessageQueueException(String message, Throwable cause) {
        super(message, cause);
    }
}
