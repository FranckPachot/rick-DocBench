package com.docbench.adapter.oracle;

import com.docbench.adapter.spi.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * TDD Unit Tests for OracleOsonAdapter.
 * Tests the Oracle OSON (binary JSON) adapter with O(1) hash-indexed field access.
 */
@DisplayName("OracleOsonAdapter")
class OracleOsonAdapterTest {

    private OracleOsonAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new OracleOsonAdapter();
    }

    @Nested
    @DisplayName("Adapter Identity")
    class AdapterIdentityTests {

        @Test
        @DisplayName("should return correct adapter ID")
        void getAdapterId_shouldReturnOracleOson() {
            assertThat(adapter.getAdapterId()).isEqualTo("oracle-oson");
        }

        @Test
        @DisplayName("should return human-readable display name")
        void getDisplayName_shouldReturnReadableName() {
            assertThat(adapter.getDisplayName())
                    .isNotNull()
                    .containsIgnoringCase("Oracle")
                    .containsIgnoringCase("OSON");
        }

        @Test
        @DisplayName("should return version")
        void getVersion_shouldReturnVersion() {
            assertThat(adapter.getVersion())
                    .isNotNull()
                    .isNotEmpty();
        }
    }

    @Nested
    @DisplayName("Capabilities")
    class CapabilitiesTests {

        @Test
        @DisplayName("should support nested document access")
        void capabilities_shouldSupportNestedDocuments() {
            assertThat(adapter.hasCapability(Capability.NESTED_DOCUMENT_ACCESS)).isTrue();
        }

        @Test
        @DisplayName("should support array index access")
        void capabilities_shouldSupportArrayIndex() {
            assertThat(adapter.hasCapability(Capability.ARRAY_INDEX_ACCESS)).isTrue();
        }

        @Test
        @DisplayName("should support partial document retrieval")
        void capabilities_shouldSupportPartialRetrieval() {
            assertThat(adapter.hasCapability(Capability.PARTIAL_DOCUMENT_RETRIEVAL)).isTrue();
        }

        @Test
        @DisplayName("should claim SERVER_TRAVERSAL_TIME (OSON advantage)")
        void capabilities_shouldClaimServerTraversalTime() {
            // OSON provides O(1) field access via hash index - can report traversal time
            assertThat(adapter.hasCapability(Capability.SERVER_TRAVERSAL_TIME)).isTrue();
        }

        @Test
        @DisplayName("should support server execution time")
        void capabilities_shouldSupportServerExecutionTime() {
            assertThat(adapter.hasCapability(Capability.SERVER_EXECUTION_TIME)).isTrue();
        }

        @Test
        @DisplayName("should support explain plan")
        void capabilities_shouldSupportExplainPlan() {
            assertThat(adapter.hasCapability(Capability.EXPLAIN_PLAN)).isTrue();
        }

        @Test
        @DisplayName("should support bulk insert")
        void capabilities_shouldSupportBulkInsert() {
            assertThat(adapter.hasCapability(Capability.BULK_INSERT)).isTrue();
        }

        @Test
        @DisplayName("should support client timing hooks")
        void capabilities_shouldSupportClientTimingHooks() {
            assertThat(adapter.hasCapability(Capability.CLIENT_TIMING_HOOKS)).isTrue();
        }

        @Test
        @DisplayName("hasCapability should return true for supported")
        void hasCapability_whenSupported_shouldReturnTrue() {
            assertThat(adapter.hasCapability(Capability.NESTED_DOCUMENT_ACCESS)).isTrue();
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
                    Capability.SHARDING  // Not supported by Oracle adapter
            ))).isFalse();
        }
    }

    @Nested
    @DisplayName("Configuration Validation")
    class ConfigurationValidationTests {

        @Test
        @DisplayName("should validate valid JDBC URL")
        void validateConfig_withValidUrl_shouldPass() {
            ConnectionConfig config = ConnectionConfig.builder()
                    .uri("jdbc:oracle:thin:@localhost:1521/FREEPDB1")
                    .database("testdb")
                    .option("username", "user")
                    .option("password", "pass")
                    .build();

            ValidationResult result = adapter.validateConfig(config);

            assertThat(result.isValid()).isTrue();
        }

        @Test
        @DisplayName("should reject empty URL")
        void validateConfig_withEmptyUrl_shouldFail() {
            ConnectionConfig config = ConnectionConfig.builder()
                    .uri("")
                    .database("testdb")
                    .build();

            ValidationResult result = adapter.validateConfig(config);

            assertThat(result.isInvalid()).isTrue();
        }

        @Test
        @DisplayName("should reject invalid URL scheme")
        void validateConfig_withInvalidScheme_shouldFail() {
            ConnectionConfig config = ConnectionConfig.builder()
                    .uri("mongodb://localhost:27017/test")
                    .database("testdb")
                    .build();

            ValidationResult result = adapter.validateConfig(config);

            assertThat(result.isInvalid()).isTrue();
            assertThat(result.allErrorMessages()).containsIgnoringCase("jdbc");
        }

        @Test
        @DisplayName("should accept TNS connection string")
        void validateConfig_withTnsUrl_shouldPass() {
            ConnectionConfig config = ConnectionConfig.builder()
                    .uri("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))")
                    .database("testdb")
                    .option("username", "user")
                    .option("password", "pass")
                    .build();

            ValidationResult result = adapter.validateConfig(config);

            assertThat(result.isValid()).isTrue();
        }
    }

    @Nested
    @DisplayName("Configuration Options")
    class ConfigurationOptionsTests {

        @Test
        @DisplayName("should expose Oracle-specific options")
        void getConfigurationOptions_shouldIncludeOracleOptions() {
            var options = adapter.getConfigurationOptions();

            assertThat(options)
                    .containsKey("username")
                    .containsKey("password")
                    .containsKey("poolSize");
        }
    }

    @Nested
    @DisplayName("OSON-Specific Features")
    class OsonFeaturesTests {

        @Test
        @DisplayName("should report O(1) access pattern capability")
        void osonFeatures_shouldSupportHashIndexedAccess() {
            // OSON uses hash-indexed field lookup
            assertThat(adapter.supportsHashIndexedFieldAccess()).isTrue();
        }

        @Test
        @DisplayName("should support JSON duality views")
        void osonFeatures_shouldSupportDualityViews() {
            // Oracle 23c JSON Duality Views
            assertThat(adapter.supportsJsonDualityViews()).isTrue();
        }

        @Test
        @DisplayName("should support JSON path expressions")
        void osonFeatures_shouldSupportJsonPath() {
            assertThat(adapter.supportsJsonPathExpressions()).isTrue();
        }
    }
}
