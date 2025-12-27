package com.docbench.adapter.spi;

/**
 * Exception thrown when a database connection fails.
 */
public class ConnectionException extends DocBenchException {

    private final String adapterId;

    public ConnectionException(String adapterId, String message) {
        super(message);
        this.adapterId = adapterId;
    }

    public ConnectionException(String adapterId, String message, Throwable cause) {
        super(message, cause);
        this.adapterId = adapterId;
    }

    public String getAdapterId() {
        return adapterId;
    }
}
