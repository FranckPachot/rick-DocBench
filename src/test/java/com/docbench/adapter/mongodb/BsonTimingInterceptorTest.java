package com.docbench.adapter.mongodb;

import com.docbench.metrics.MetricsCollector;
import com.docbench.util.MockTimeSource;
import com.docbench.util.TimeSource;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static com.docbench.util.TestDurations.micros;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * TDD Unit Tests for BsonTimingInterceptor.
 * This interceptor captures MongoDB command timing for overhead analysis.
 */
@DisplayName("BsonTimingInterceptor")
class BsonTimingInterceptorTest {

    private MockTimeSource timeSource;
    private MetricsCollector collector;
    private BsonTimingInterceptor interceptor;

    @BeforeEach
    void setUp() {
        timeSource = TimeSource.mock(0L);
        collector = new MetricsCollector(timeSource);
        interceptor = new BsonTimingInterceptor(collector, timeSource);
    }

    @Nested
    @DisplayName("Command Started")
    class CommandStartedTests {

        @Test
        @DisplayName("should record start time for command")
        void commandStarted_shouldRecordStartTime() {
            CommandStartedEvent event = createStartedEvent(1, "find");

            assertThatNoException().isThrownBy(() ->
                interceptor.commandStarted(event)
            );
        }

        @Test
        @DisplayName("should handle multiple concurrent commands")
        void commandStarted_shouldHandleConcurrent() {
            CommandStartedEvent event1 = createStartedEvent(1, "find");
            CommandStartedEvent event2 = createStartedEvent(2, "insert");

            interceptor.commandStarted(event1);
            timeSource.advance(micros(100));
            interceptor.commandStarted(event2);

            // Both should be tracked independently
            assertThatNoException().isThrownBy(() -> {
                interceptor.commandSucceeded(createSucceededEvent(1, "find", 50));
                interceptor.commandSucceeded(createSucceededEvent(2, "insert", 30));
            });
        }
    }

    @Nested
    @DisplayName("Command Succeeded")
    class CommandSucceededTests {

        @Test
        @DisplayName("should record client round trip time")
        void commandSucceeded_shouldRecordClientRoundTrip() {
            CommandStartedEvent startEvent = createStartedEvent(1, "find");
            interceptor.commandStarted(startEvent);

            timeSource.advance(micros(500));

            CommandSucceededEvent successEvent = createSucceededEvent(1, "find", 200);
            interceptor.commandSucceeded(successEvent);

            var summary = collector.summarize();
            assertThat(summary.hasMetric("mongodb.client_round_trip")).isTrue();
            assertThat(summary.get("mongodb.client_round_trip").mean())
                    .isCloseTo(500_000.0, within(1000.0)); // 500 microseconds in nanos, with HdrHistogram precision tolerance
        }

        @Test
        @DisplayName("should record server execution time")
        void commandSucceeded_shouldRecordServerExecution() {
            CommandStartedEvent startEvent = createStartedEvent(1, "find");
            interceptor.commandStarted(startEvent);
            timeSource.advance(micros(500));

            // Server reports 200 microseconds execution time
            CommandSucceededEvent successEvent = createSucceededEvent(1, "find", 200);
            interceptor.commandSucceeded(successEvent);

            var summary = collector.summarize();
            assertThat(summary.hasMetric("mongodb.server_execution")).isTrue();
        }

        @Test
        @DisplayName("should calculate overhead")
        void commandSucceeded_shouldCalculateOverhead() {
            CommandStartedEvent startEvent = createStartedEvent(1, "find");
            interceptor.commandStarted(startEvent);

            timeSource.advance(micros(500));

            // Server says 200us, client sees 500us, so 300us overhead
            CommandSucceededEvent successEvent = createSucceededEvent(1, "find", 200);
            interceptor.commandSucceeded(successEvent);

            var summary = collector.summarize();
            assertThat(summary.hasMetric("mongodb.overhead")).isTrue();
        }

        @Test
        @DisplayName("should increment operation counter")
        void commandSucceeded_shouldIncrementCounter() {
            interceptor.commandStarted(createStartedEvent(1, "find"));
            interceptor.commandSucceeded(createSucceededEvent(1, "find", 100));

            interceptor.commandStarted(createStartedEvent(2, "find"));
            interceptor.commandSucceeded(createSucceededEvent(2, "find", 100));

            assertThat(interceptor.getCompletedOperationCount()).isEqualTo(2);
        }

        @Test
        @DisplayName("should track command types separately")
        void commandSucceeded_shouldTrackByCommandType() {
            interceptor.commandStarted(createStartedEvent(1, "find"));
            timeSource.advance(micros(100));
            interceptor.commandSucceeded(createSucceededEvent(1, "find", 50));

            interceptor.commandStarted(createStartedEvent(2, "insert"));
            timeSource.advance(micros(200));
            interceptor.commandSucceeded(createSucceededEvent(2, "insert", 100));

            var summary = collector.summarize();
            assertThat(summary.hasMetric("mongodb.find.client_round_trip")).isTrue();
            assertThat(summary.hasMetric("mongodb.insert.client_round_trip")).isTrue();
        }
    }

    @Nested
    @DisplayName("Command Failed")
    class CommandFailedTests {

        @Test
        @DisplayName("should record failure")
        void commandFailed_shouldRecordFailure() {
            interceptor.commandStarted(createStartedEvent(1, "find"));
            timeSource.advance(micros(100));

            CommandFailedEvent failEvent = createFailedEvent(1, "find",
                    new RuntimeException("Connection lost"));
            interceptor.commandFailed(failEvent);

            assertThat(interceptor.getFailedOperationCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("should still record timing for failed commands")
        void commandFailed_shouldRecordTiming() {
            interceptor.commandStarted(createStartedEvent(1, "find"));
            timeSource.advance(micros(100));

            CommandFailedEvent failEvent = createFailedEvent(1, "find",
                    new RuntimeException("Error"));
            interceptor.commandFailed(failEvent);

            var summary = collector.summarize();
            assertThat(summary.hasMetric("mongodb.failed.client_round_trip")).isTrue();
        }
    }

    @Nested
    @DisplayName("Statistics")
    class StatisticsTests {

        @Test
        @DisplayName("should track total operations")
        void getTotalOperationCount_shouldTrackAll() {
            interceptor.commandStarted(createStartedEvent(1, "find"));
            interceptor.commandSucceeded(createSucceededEvent(1, "find", 100));

            interceptor.commandStarted(createStartedEvent(2, "insert"));
            interceptor.commandFailed(createFailedEvent(2, "insert", new RuntimeException()));

            assertThat(interceptor.getTotalOperationCount()).isEqualTo(2);
            assertThat(interceptor.getCompletedOperationCount()).isEqualTo(1);
            assertThat(interceptor.getFailedOperationCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("should reset statistics")
        void reset_shouldClearStatistics() {
            interceptor.commandStarted(createStartedEvent(1, "find"));
            interceptor.commandSucceeded(createSucceededEvent(1, "find", 100));

            interceptor.reset();

            assertThat(interceptor.getTotalOperationCount()).isZero();
            assertThat(interceptor.getCompletedOperationCount()).isZero();
        }
    }

    @Nested
    @DisplayName("Pending Commands")
    class PendingCommandsTests {

        @Test
        @DisplayName("should track pending commands")
        void getPendingCommandCount_shouldTrackInFlight() {
            interceptor.commandStarted(createStartedEvent(1, "find"));
            interceptor.commandStarted(createStartedEvent(2, "insert"));

            assertThat(interceptor.getPendingCommandCount()).isEqualTo(2);

            interceptor.commandSucceeded(createSucceededEvent(1, "find", 100));

            assertThat(interceptor.getPendingCommandCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("should handle orphaned started events gracefully")
        void commandSucceeded_withoutStart_shouldNotFail() {
            // Succeed event without corresponding start
            CommandSucceededEvent orphanEvent = createSucceededEvent(999, "find", 100);

            assertThatNoException().isThrownBy(() ->
                interceptor.commandSucceeded(orphanEvent)
            );
        }
    }

    // Helper methods to create mock events

    private CommandStartedEvent createStartedEvent(int requestId, String commandName) {
        BsonDocument command = new BsonDocument(commandName, new BsonInt32(1));
        return new CommandStartedEvent(
                null,  // requestContext
                1,     // operationId
                requestId,
                null,  // connectionDescription
                "testdb",
                commandName,
                command
        );
    }

    private CommandSucceededEvent createSucceededEvent(int requestId, String commandName, long elapsedMicros) {
        BsonDocument response = new BsonDocument("ok", new BsonInt32(1));
        return new CommandSucceededEvent(
                null,  // requestContext
                1,     // operationId
                requestId,
                null,  // connectionDescription
                "testdb",
                commandName,
                response,
                elapsedMicros * 1000  // Convert to nanos for the event
        );
    }

    private CommandFailedEvent createFailedEvent(int requestId, String commandName, Throwable error) {
        return new CommandFailedEvent(
                null,  // requestContext
                1,     // operationId
                requestId,
                null,  // connectionDescription
                "testdb",
                commandName,
                100_000L,  // elapsed nanos
                error
        );
    }
}
