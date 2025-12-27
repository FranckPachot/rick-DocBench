package com.docbench.adapter.spi;

/**
 * Exception thrown when a capability is not supported.
 */
public class CapabilityNotSupportedException extends DocBenchException {

    private final Capability capability;
    private final String adapterId;

    public CapabilityNotSupportedException(Capability capability, String adapterId) {
        super("Capability " + capability + " not supported by adapter: " + adapterId);
        this.capability = capability;
        this.adapterId = adapterId;
    }

    public Capability getCapability() {
        return capability;
    }

    public String getAdapterId() {
        return adapterId;
    }
}
