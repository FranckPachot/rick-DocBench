package com.docbench.adapter.mongodb;

import com.docbench.metrics.MetricsCollector;
import com.docbench.util.MockTimeSource;
import com.docbench.util.TimeSource;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.docbench.util.TestDurations.micros;
import static org.assertj.core.api.Assertions.*;

/**
 * TDD Tests for BSON Deserialization Timer.
 * Captures field traversal timing for overhead decomposition.
 */
@DisplayName("BsonDeserializationTimer")
class BsonDeserializationTimerTest {

    private MockTimeSource timeSource;
    private MetricsCollector collector;
    private BsonDeserializationTimer timer;

    @BeforeEach
    void setUp() {
        timeSource = TimeSource.mock(0L);
        collector = new MetricsCollector(timeSource);
        timer = new BsonDeserializationTimer(collector, timeSource);
    }

    @Nested
    @DisplayName("Field Access Timing")
    class FieldAccessTimingTests {

        @Test
        @DisplayName("should time single field access")
        void timeFieldAccess_shouldRecordDuration() {
            Document doc = new Document("field1", "value1");

            timer.startDeserialization("op-1");
            timeSource.advance(micros(10));
            timer.recordFieldAccess("op-1", "field1");
            timeSource.advance(micros(5));
            timer.endDeserialization("op-1");

            var summary = collector.summarize();
            assertThat(summary.hasMetric("bson.field_access.field1")).isTrue();
        }

        @Test
        @DisplayName("should accumulate multiple field accesses")
        void timeFieldAccess_shouldAccumulate() {
            timer.startDeserialization("op-1");

            timeSource.advance(micros(10));
            timer.recordFieldAccess("op-1", "field1");

            timeSource.advance(micros(20));
            timer.recordFieldAccess("op-1", "field2");

            timeSource.advance(micros(15));
            timer.recordFieldAccess("op-1", "field3");

            timer.endDeserialization("op-1");

            assertThat(timer.getFieldAccessCount("op-1")).isEqualTo(3);
        }

        @Test
        @DisplayName("should track field position in traversal")
        void timeFieldAccess_shouldTrackPosition() {
            timer.startDeserialization("op-1");

            timer.recordFieldAccess("op-1", "first", 0);
            timer.recordFieldAccess("op-1", "second", 1);
            timer.recordFieldAccess("op-1", "third", 2);

            timer.endDeserialization("op-1");

            // Later fields should show O(n) traversal pattern
            assertThat(timer.getFieldPosition("op-1", "third")).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Nested Document Timing")
    class NestedDocumentTimingTests {

        @Test
        @DisplayName("should time nested document access")
        void timeNestedAccess_shouldRecordDepth() {
            timer.startDeserialization("op-1");

            timer.enterNestedDocument("op-1", "customer");
            timeSource.advance(micros(25));
            timer.recordFieldAccess("op-1", "customer.name");
            timer.exitNestedDocument("op-1");

            timer.endDeserialization("op-1");

            assertThat(timer.getMaxNestingDepth("op-1")).isEqualTo(1);
        }

        @Test
        @DisplayName("should track deeply nested access")
        void timeNestedAccess_shouldTrackDeepNesting() {
            timer.startDeserialization("op-1");

            timer.enterNestedDocument("op-1", "level1");
            timer.enterNestedDocument("op-1", "level2");
            timer.enterNestedDocument("op-1", "level3");
            timeSource.advance(micros(50));
            timer.recordFieldAccess("op-1", "level1.level2.level3.value");
            timer.exitNestedDocument("op-1");
            timer.exitNestedDocument("op-1");
            timer.exitNestedDocument("op-1");

            timer.endDeserialization("op-1");

            assertThat(timer.getMaxNestingDepth("op-1")).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Array Access Timing")
    class ArrayAccessTimingTests {

        @Test
        @DisplayName("should time array element access")
        void timeArrayAccess_shouldRecordIndex() {
            timer.startDeserialization("op-1");

            timer.enterArray("op-1", "items");
            timeSource.advance(micros(5));
            timer.recordArrayElementAccess("op-1", "items", 0);
            timeSource.advance(micros(5));
            timer.recordArrayElementAccess("op-1", "items", 1);
            timeSource.advance(micros(5));
            timer.recordArrayElementAccess("op-1", "items", 2);
            timer.exitArray("op-1");

            timer.endDeserialization("op-1");

            assertThat(timer.getArrayElementCount("op-1", "items")).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Deserialization Summary")
    class DeserializationSummaryTests {

        @Test
        @DisplayName("should calculate total deserialization time")
        void endDeserialization_shouldCalculateTotalTime() {
            timer.startDeserialization("op-1");
            timeSource.advance(micros(100));
            timer.endDeserialization("op-1");

            Duration total = timer.getTotalDeserializationTime("op-1");
            assertThat(total).isEqualTo(micros(100));
        }

        @Test
        @DisplayName("should provide deserialization breakdown")
        void getBreakdown_shouldReturnDetails() {
            timer.startDeserialization("op-1");

            timeSource.advance(micros(10));
            timer.recordFieldAccess("op-1", "field1", 0);

            timer.enterNestedDocument("op-1", "nested");
            timeSource.advance(micros(30));
            timer.recordFieldAccess("op-1", "nested.inner", 0);
            timer.exitNestedDocument("op-1");

            timeSource.advance(micros(10));
            timer.endDeserialization("op-1");

            var breakdown = timer.getDeserializationBreakdown("op-1");

            assertThat(breakdown).isNotNull();
            assertThat(breakdown.totalTime()).isEqualTo(micros(50));
            assertThat(breakdown.fieldCount()).isEqualTo(2);
            assertThat(breakdown.maxNestingDepth()).isEqualTo(1);
        }

        @Test
        @DisplayName("should record metrics to collector")
        void endDeserialization_shouldRecordMetrics() {
            timer.startDeserialization("op-1");
            timeSource.advance(micros(75));
            timer.recordFieldAccess("op-1", "field1", 0);
            timeSource.advance(micros(25));
            timer.endDeserialization("op-1");

            var summary = collector.summarize();
            assertThat(summary.hasMetric("bson.deserialization.total")).isTrue();
            // field_count is a counter, not a timing metric
            assertThat(summary.getCounter("bson.deserialization.field_count")).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Concurrent Operations")
    class ConcurrentOperationsTests {

        @Test
        @DisplayName("should track multiple operations independently")
        void multipleOperations_shouldBeIndependent() {
            timer.startDeserialization("op-1");
            timer.startDeserialization("op-2");

            timeSource.advance(micros(50));
            timer.recordFieldAccess("op-1", "field1", 0);

            timeSource.advance(micros(100));
            timer.recordFieldAccess("op-2", "fieldA", 0);
            timer.recordFieldAccess("op-2", "fieldB", 1);

            timer.endDeserialization("op-1");
            timer.endDeserialization("op-2");

            assertThat(timer.getFieldAccessCount("op-1")).isEqualTo(1);
            assertThat(timer.getFieldAccessCount("op-2")).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Traversal Pattern Analysis")
    class TraversalPatternTests {

        @Test
        @DisplayName("should detect O(n) field scanning pattern")
        void fieldAccess_shouldShowLinearPattern() {
            // Simulate accessing fields at different positions
            // Later fields take longer due to O(n) scanning
            timer.startDeserialization("op-1");

            // First field - fast
            timeSource.advance(micros(10));
            timer.recordFieldAccess("op-1", "field_001", 0);

            // Middle field - medium
            timeSource.advance(micros(50));
            timer.recordFieldAccess("op-1", "field_050", 49);

            // Last field - slow
            timeSource.advance(micros(100));
            timer.recordFieldAccess("op-1", "field_100", 99);

            timer.endDeserialization("op-1");

            var breakdown = timer.getDeserializationBreakdown("op-1");

            // Verify we captured the access pattern
            assertThat(breakdown.fieldCount()).isEqualTo(3);
            assertThat(breakdown.lastFieldPosition()).isEqualTo(99);
        }
    }
}
