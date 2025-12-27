package com.docbench.adapter.spi;

import java.util.Objects;

/**
 * Update operation for modifying documents.
 */
public record UpdateOperation(
        String operationId,
        String documentId,
        String updatePath,
        Object newValue,
        boolean upsert
) implements Operation {

    public UpdateOperation {
        Objects.requireNonNull(operationId, "operationId must not be null");
        Objects.requireNonNull(documentId, "documentId must not be null");
        Objects.requireNonNull(updatePath, "updatePath must not be null");
    }

    @Override
    public OperationType type() {
        return OperationType.UPDATE;
    }
}
