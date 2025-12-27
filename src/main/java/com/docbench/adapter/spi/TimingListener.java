package com.docbench.adapter.spi;

/**
 * Callback interface for fine-grained timing capture.
 */
public interface TimingListener {

    /**
     * Called when serialization starts.
     */
    void onSerializationStart(String operationId);

    /**
     * Called when serialization completes.
     */
    void onSerializationComplete(String operationId, int bytesSerialized);

    /**
     * Called when wire transmission starts.
     */
    void onWireTransmitStart(String operationId);

    /**
     * Called when wire transmission completes.
     */
    void onWireTransmitComplete(String operationId, int bytesSent);

    /**
     * Called when wire reception starts.
     */
    void onWireReceiveStart(String operationId);

    /**
     * Called when wire reception completes.
     */
    void onWireReceiveComplete(String operationId, int bytesReceived);

    /**
     * Called when deserialization starts.
     */
    void onDeserializationStart(String operationId);

    /**
     * Called when deserialization completes.
     */
    void onDeserializationComplete(String operationId, int fieldsDeserialized);
}
