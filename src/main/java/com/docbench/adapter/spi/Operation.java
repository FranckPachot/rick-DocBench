package com.docbench.adapter.spi;

/**
 * Sealed interface for type-safe operation definitions.
 * Each operation type captures the data needed for execution and timing.
 */
public sealed interface Operation
        permits InsertOperation, ReadOperation, UpdateOperation, DeleteOperation, AggregateOperation {

    /**
     * Unique identifier for this operation instance.
     * Used for correlating timing events and results.
     */
    String operationId();

    /**
     * The type of this operation.
     */
    OperationType type();
}
