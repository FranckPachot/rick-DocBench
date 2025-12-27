package com.docbench.adapter.spi;

/**
 * Base exception for DocBench errors.
 */
public class DocBenchException extends RuntimeException {

    public DocBenchException(String message) {
        super(message);
    }

    public DocBenchException(String message, Throwable cause) {
        super(message, cause);
    }
}
