package com.docbench.adapter.spi;

import java.util.List;
import java.util.Objects;

/**
 * Aggregate operation for complex queries.
 */
public record AggregateOperation(
        String operationId,
        List<String> pipeline,
        boolean explain
) implements Operation {

    public AggregateOperation {
        Objects.requireNonNull(operationId, "operationId must not be null");
        Objects.requireNonNull(pipeline, "pipeline must not be null");
        pipeline = List.copyOf(pipeline);
    }

    @Override
    public OperationType type() {
        return OperationType.AGGREGATE;
    }
}
