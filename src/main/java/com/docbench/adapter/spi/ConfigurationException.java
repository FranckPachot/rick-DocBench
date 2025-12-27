package com.docbench.adapter.spi;

/**
 * Exception thrown when configuration is invalid.
 */
public class ConfigurationException extends DocBenchException {

    private final ValidationResult validationResult;

    public ConfigurationException(String message) {
        super(message);
        this.validationResult = ValidationResult.failure("config", message);
    }

    public ConfigurationException(ValidationResult validationResult) {
        super("Configuration validation failed: " + validationResult.allErrorMessages());
        this.validationResult = validationResult;
    }

    public ValidationResult getValidationResult() {
        return validationResult;
    }
}
