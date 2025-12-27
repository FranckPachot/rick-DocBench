package com.docbench.adapter.mongodb;

import com.docbench.adapter.spi.ConnectionTimingMetrics;
import com.docbench.adapter.spi.TimingListener;
import com.docbench.metrics.MetricsCollector;
import com.docbench.util.TimeSource;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * TDD Unit Tests for MongoDBInstrumentedConnection.
 */
@DisplayName("MongoDBInstrumentedConnection")
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MongoDBInstrumentedConnectionTest {

    @Mock
    private MongoClient mockClient;

    @Mock
    private MongoDatabase mockDatabase;

    private MetricsCollector metricsCollector;
    private MongoDBInstrumentedConnection connection;

    @BeforeEach
    void setUp() {
        metricsCollector = new MetricsCollector(TimeSource.system());
        when(mockClient.getDatabase(anyString())).thenReturn(mockDatabase);
        connection = new MongoDBInstrumentedConnection(
                mockClient,
                "testdb",
                metricsCollector
        );
    }

    @Nested
    @DisplayName("Connection Identity")
    class ConnectionIdentityTests {

        @Test
        @DisplayName("should have unique connection ID")
        void getConnectionId_shouldReturnUniqueId() {
            assertThat(connection.getConnectionId())
                    .isNotNull()
                    .isNotEmpty();
        }

        @Test
        @DisplayName("should generate different IDs for different connections")
        void getConnectionId_shouldBeUniquePerConnection() {
            MongoDBInstrumentedConnection other = new MongoDBInstrumentedConnection(
                    mockClient, "testdb", metricsCollector
            );

            assertThat(connection.getConnectionId())
                    .isNotEqualTo(other.getConnectionId());
        }
    }

    @Nested
    @DisplayName("Unwrap")
    class UnwrapTests {

        @Test
        @DisplayName("should unwrap to MongoClient")
        void unwrap_toMongoClient_shouldSucceed() {
            MongoClient unwrapped = connection.unwrap(MongoClient.class);

            assertThat(unwrapped).isSameAs(mockClient);
        }

        @Test
        @DisplayName("should throw for incompatible type")
        void unwrap_toIncompatibleType_shouldThrow() {
            assertThatThrownBy(() -> connection.unwrap(String.class))
                    .isInstanceOf(ClassCastException.class);
        }
    }

    @Nested
    @DisplayName("Validity")
    class ValidityTests {

        @Test
        @DisplayName("should be valid after creation")
        void isValid_afterCreation_shouldBeTrue() {
            assertThat(connection.isValid()).isTrue();
        }

        @Test
        @DisplayName("should be invalid after close")
        void isValid_afterClose_shouldBeFalse() {
            connection.close();

            assertThat(connection.isValid()).isFalse();
        }
    }

    @Nested
    @DisplayName("Metrics Collector")
    class MetricsCollectorTests {

        @Test
        @DisplayName("should return associated metrics collector")
        void getMetricsCollector_shouldReturnCollector() {
            assertThat(connection.getMetricsCollector())
                    .isSameAs(metricsCollector);
        }
    }

    @Nested
    @DisplayName("Timing Listeners")
    class TimingListenerTests {

        @Mock
        private TimingListener listener;

        @Test
        @DisplayName("should add timing listener")
        void addTimingListener_shouldAdd() {
            assertThatNoException().isThrownBy(() ->
                connection.addTimingListener(listener)
            );
        }

        @Test
        @DisplayName("should remove timing listener")
        void removeTimingListener_shouldRemove() {
            connection.addTimingListener(listener);

            assertThatNoException().isThrownBy(() ->
                connection.removeTimingListener(listener)
            );
        }
    }

    @Nested
    @DisplayName("Timing Metrics")
    class TimingMetricsTests {

        @Test
        @DisplayName("should return empty metrics initially")
        void getTimingMetrics_initially_shouldBeEmpty() {
            ConnectionTimingMetrics metrics = connection.getTimingMetrics();

            assertThat(metrics.operationCount()).isZero();
            assertThat(metrics.serializationTimeNanos()).isZero();
            assertThat(metrics.deserializationTimeNanos()).isZero();
        }

        @Test
        @DisplayName("should reset timing metrics")
        void resetTimingMetrics_shouldClear() {
            // Simulate some activity would happen here in real usage
            connection.resetTimingMetrics();

            ConnectionTimingMetrics metrics = connection.getTimingMetrics();
            assertThat(metrics.operationCount()).isZero();
        }
    }

    @Nested
    @DisplayName("Database Access")
    class DatabaseAccessTests {

        @Test
        @DisplayName("should provide database accessor")
        void getDatabase_shouldReturnDatabase() {
            MongoDatabase db = connection.getDatabase();

            assertThat(db).isSameAs(mockDatabase);
        }
    }

    @Nested
    @DisplayName("Close")
    class CloseTests {

        @Test
        @DisplayName("should close underlying client")
        void close_shouldCloseClient() {
            connection.close();

            verify(mockClient).close();
        }

        @Test
        @DisplayName("should be idempotent")
        void close_multipleTimes_shouldBeIdempotent() {
            connection.close();
            connection.close();

            // Should only close once
            verify(mockClient, times(1)).close();
        }
    }
}
