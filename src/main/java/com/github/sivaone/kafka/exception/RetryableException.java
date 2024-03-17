package com.github.sivaone.kafka.exception;

/**
 * Throwing this error will resent in message getting retired for specific number of time and then to Dead letter topic
 */
public class RetryableException extends RuntimeException {
    public RetryableException(String message) {
        super(message);
    }
}

