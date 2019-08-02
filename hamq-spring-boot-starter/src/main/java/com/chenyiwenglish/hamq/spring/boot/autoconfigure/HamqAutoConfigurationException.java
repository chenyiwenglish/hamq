package com.chenyiwenglish.hamq.spring.boot.autoconfigure;

public class HamqAutoConfigurationException extends RuntimeException {
    public HamqAutoConfigurationException(String message) {
        super(message);
    }

    public HamqAutoConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
