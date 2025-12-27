package com.docbench.adapter.spi;

/**
 * Read preference for replica set deployments.
 */
public enum ReadPreference {
    /**
     * Read from primary only.
     */
    PRIMARY,

    /**
     * Read from primary preferred, secondary if unavailable.
     */
    PRIMARY_PREFERRED,

    /**
     * Read from secondary only.
     */
    SECONDARY,

    /**
     * Read from secondary preferred, primary if unavailable.
     */
    SECONDARY_PREFERRED,

    /**
     * Read from nearest member.
     */
    NEAREST
}
