package com.docbench.adapter.spi;

/**
 * Exception thrown when an operation fails.
 */
public class OperationException extends DocBenchException {

    private final String operationId;
    private final OperationType operationType;

    public OperationException(String operationId, OperationType type, String message) {
        super(message);
        this.operationId = operationId;
        this.operationType = type;
    }

    public OperationException(String operationId, OperationType type, String message, Throwable cause) {
        super(message, cause);
        this.operationId = operationId;
        this.operationType = type;
    }

    public String getOperationId() {
        return operationId;
    }

    public OperationType getOperationType() {
        return operationType;
    }
}
