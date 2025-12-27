package com.docbench.adapter.spi;

import java.util.Objects;

/**
 * Delete operation for removing documents.
 */
public record DeleteOperation(
        String operationId,
        String documentId
) implements Operation {

    public DeleteOperation {
        Objects.requireNonNull(operationId, "operationId must not be null");
        Objects.requireNonNull(documentId, "documentId must not be null");
    }

    @Override
    public OperationType type() {
        return OperationType.DELETE;
    }
}
