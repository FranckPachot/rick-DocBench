package com.docbench.adapter.spi;

/**
 * Exception thrown when test environment setup fails.
 */
public class SetupException extends DocBenchException {

    public SetupException(String message) {
        super(message);
    }

    public SetupException(String message, Throwable cause) {
        super(message, cause);
    }
}
