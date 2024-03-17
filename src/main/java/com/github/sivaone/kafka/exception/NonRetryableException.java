package com.github.sivaone.kafka.exception;

/**
 * Throwing this error will resent in message going straight to Dead letter topic
 */
public class NonRetryableException extends RuntimeException {
    public NonRetryableException(String message) {
        super(message);
    }
}

