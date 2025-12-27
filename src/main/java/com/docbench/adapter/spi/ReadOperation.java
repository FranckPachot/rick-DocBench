package com.docbench.adapter.spi;

import java.util.List;
import java.util.Objects;

/**
 * Read operation for retrieving documents.
 */
public record ReadOperation(
        String operationId,
        String documentId,
        List<String> projectionPaths,  // Empty = full document
        ReadPreference readPreference
) implements Operation {

    public ReadOperation {
        Objects.requireNonNull(operationId, "operationId must not be null");
        Objects.requireNonNull(documentId, "documentId must not be null");
        Objects.requireNonNull(projectionPaths, "projectionPaths must not be null");
        Objects.requireNonNull(readPreference, "readPreference must not be null");
        projectionPaths = List.copyOf(projectionPaths);
    }

    /**
     * Creates a read operation for full document retrieval.
     */
    public static ReadOperation fullDocument(String operationId, String documentId) {
        return new ReadOperation(operationId, documentId, List.of(), ReadPreference.PRIMARY);
    }

    /**
     * Creates a read operation with projection.
     */
    public static ReadOperation withProjection(String operationId, String documentId, List<String> paths) {
        return new ReadOperation(operationId, documentId, paths, ReadPreference.PRIMARY);
    }

    @Override
    public OperationType type() {
        return OperationType.READ;
    }

    /**
     * Returns true if this is a partial document retrieval.
     */
    public boolean hasProjection() {
        return !projectionPaths.isEmpty();
    }
}
