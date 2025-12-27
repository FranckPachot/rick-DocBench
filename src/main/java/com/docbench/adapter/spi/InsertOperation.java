package com.docbench.adapter.spi;

import java.util.Objects;

/**
 * Insert operation for creating new documents.
 */
public record InsertOperation(
        String operationId,
        JsonDocument document
) implements Operation {

    public InsertOperation {
        Objects.requireNonNull(operationId, "operationId must not be null");
        Objects.requireNonNull(document, "document must not be null");
    }

    @Override
    public OperationType type() {
        return OperationType.INSERT;
    }
}
