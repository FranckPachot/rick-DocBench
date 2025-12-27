package com.docbench.adapter.mongodb;

import com.docbench.adapter.spi.*;
import com.docbench.metrics.MetricsCollector;
import com.docbench.metrics.OverheadBreakdown;
import com.docbench.util.TimeSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * TDD Unit Tests for MongoDBAdapter.
 * Written BEFORE implementation (RED phase).
 */
@DisplayName("MongoDBAdapter")
class MongoDBAdapterTest {

    private MongoDBAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new MongoDBAdapter();
    }

    @Nested
    @DisplayName("Adapter Identity")
    class AdapterIdentityTests {

        @Test
        @DisplayName("should return correct adapter ID")
        void getAdapterId_shouldReturnMongodb() {
            assertThat(adapter.getAdapterId()).isEqualTo("mongodb");
        }

        @Test
        @DisplayName("should return human-readable display name")
        void getDisplayName_shouldReturnReadableName() {
            assertThat(adapter.getDisplayName()).isEqualTo("MongoDB");
        }

        @Test
        @DisplayName("should return version")
        void getVersion_shouldReturnVersion() {
            assertThat(adapter.getVersion()).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Capabilities")
    class CapabilitiesTests {

        @Test
        @DisplayName("should support nested document access")
        void getCapabilities_shouldSupportNestedAccess() {
            assertThat(adapter.getCapabilities())
                    .contains(Capability.NESTED_DOCUMENT_ACCESS);
        }

        @Test
        @DisplayName("should support array index access")
        void getCapabilities_shouldSupportArrayAccess() {
            assertThat(adapter.getCapabilities())
                    .contains(Capability.ARRAY_INDEX_ACCESS);
        }

        @Test
        @DisplayName("should support partial document retrieval")
        void getCapabilities_shouldSupportProjection() {
            assertThat(adapter.getCapabilities())
                    .contains(Capability.PARTIAL_DOCUMENT_RETRIEVAL);
        }

        @Test
        @DisplayName("should support bulk operations")
        void getCapabilities_shouldSupportBulkOps() {
            assertThat(adapter.getCapabilities())
                    .containsAll(Set.of(
                            Capability.BULK_INSERT,
                            Capability.BULK_READ
                    ));
        }

        @Test
        @DisplayName("should support server execution time")
        void getCapabilities_shouldSupportServerTiming() {
            assertThat(adapter.getCapabilities())
                    .contains(Capability.SERVER_EXECUTION_TIME);
        }

        @Test
        @DisplayName("should support explain plan")
        void getCapabilities_shouldSupportExplain() {
            assertThat(adapter.getCapabilities())
                    .contains(Capability.EXPLAIN_PLAN);
        }

        @Test
        @DisplayName("should support client timing hooks")
        void getCapabilities_shouldSupportClientHooks() {
            assertThat(adapter.getCapabilities())
                    .contains(Capability.CLIENT_TIMING_HOOKS);
        }

        @Test
        @DisplayName("should NOT claim server traversal time (BSON limitation)")
        void getCapabilities_shouldNotClaimServerTraversal() {
            // BSON doesn't expose per-field traversal timing
            assertThat(adapter.getCapabilities())
                    .doesNotContain(Capability.SERVER_TRAVERSAL_TIME);
        }

        @Test
        @DisplayName("hasCapability should return true for supported")
        void hasCapability_shouldReturnTrueForSupported() {
            assertThat(adapter.hasCapability(Capability.NESTED_DOCUMENT_ACCESS))
                    .isTrue();
        }

        @Test
        @DisplayName("hasCapability should return false for unsupported")
        void hasCapability_shouldReturnFalseForUnsupported() {
            assertThat(adapter.hasCapability(Capability.SERVER_TRAVERSAL_TIME))
                    .isFalse();
        }

        @Test
        @DisplayName("hasAllCapabilities should check all")
        void hasAllCapabilities_shouldCheckAll() {
            assertThat(adapter.hasAllCapabilities(Set.of(
                    Capability.NESTED_DOCUMENT_ACCESS,
                    Capability.ARRAY_INDEX_ACCESS
            ))).isTrue();

            assertThat(adapter.hasAllCapabilities(Set.of(
                    Capability.NESTED_DOCUMENT_ACCESS,
                    Capability.SERVER_TRAVERSAL_TIME
            ))).isFalse();
        }
    }

    @Nested
    @DisplayName("Configuration Validation")
    class ConfigValidationTests {

        @Test
        @DisplayName("should validate valid URI")
        void validateConfig_withValidUri_shouldSucceed() {
            ConnectionConfig config = ConnectionConfig.fromUri("mongodb://localhost:27017/test");

            ValidationResult result = adapter.validateConfig(config);

            assertThat(result.isValid()).isTrue();
        }

        @Test
        @DisplayName("should reject empty URI")
        void validateConfig_withEmptyUri_shouldFail() {
            ConnectionConfig config = ConnectionConfig.fromUri("");

            ValidationResult result = adapter.validateConfig(config);

            assertThat(result.isInvalid()).isTrue();
        }

        @Test
        @DisplayName("should reject invalid URI scheme")
        void validateConfig_withInvalidScheme_shouldFail() {
            ConnectionConfig config = ConnectionConfig.fromUri("http://localhost:27017");

            ValidationResult result = adapter.validateConfig(config);

            assertThat(result.isInvalid()).isTrue();
            assertThat(result.firstErrorMessage()).contains("mongodb://");
        }
    }

    @Nested
    @DisplayName("Configuration Options")
    class ConfigOptionsTests {

        @Test
        @DisplayName("should expose MongoDB-specific options")
        void getConfigurationOptions_shouldExposeOptions() {
            Map<String, String> options = adapter.getConfigurationOptions();

            assertThat(options).containsKeys(
                    "maxPoolSize",
                    "minPoolSize",
                    "connectTimeoutMs",
                    "readPreference",
                    "writeConcern"
            );
        }
    }
}
