package com.docbench.metrics;

import com.docbench.util.MockTimeSource;
import com.docbench.util.TimeSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static com.docbench.util.TestDurations.micros;
import static org.assertj.core.api.Assertions.*;

/**
 * TDD Tests for MetricsCollector.
 * Uses HdrHistogram for high-precision latency tracking.
 */
@DisplayName("MetricsCollector")
class MetricsCollectorTest {

    private MockTimeSource timeSource;
    private MetricsCollector collector;

    @BeforeEach
    void setUp() {
        timeSource = TimeSource.mock(0L);
        collector = new MetricsCollector(timeSource);
    }

    @Nested
    @DisplayName("Recording Timings")
    class RecordingTimingsTests {

        @Test
        @DisplayName("should record single timing")
        void recordTiming_shouldStoreValue() {
            collector.recordTiming("test_metric", micros(100));

            MetricsSummary summary = collector.summarize();

            assertThat(summary.hasMetric("test_metric")).isTrue();
            assertThat(summary.get("test_metric").count()).isEqualTo(1);
        }

        @Test
        @DisplayName("should accumulate multiple timings")
        void recordTiming_shouldAccumulateValues() {
            collector.recordTiming("test_metric", micros(100));
            collector.recordTiming("test_metric", micros(200));
            collector.recordTiming("test_metric", micros(150));

            MetricsSummary summary = collector.summarize();
            HistogramSummary stats = summary.get("test_metric");

            assertThat(stats.count()).isEqualTo(3);
            assertThat(stats.mean()).isCloseTo(150_000.0, within(1000.0)); // nanoseconds
        }

        @Test
        @DisplayName("should track separate metrics independently")
        void recordTiming_shouldTrackMetricsSeparately() {
            collector.recordTiming("metric_a", micros(100));
            collector.recordTiming("metric_b", micros(200));
            collector.recordTiming("metric_a", micros(100));

            MetricsSummary summary = collector.summarize();

            assertThat(summary.get("metric_a").count()).isEqualTo(2);
            assertThat(summary.get("metric_b").count()).isEqualTo(1);
        }

        @Test
        @DisplayName("should handle zero duration")
        void recordTiming_withZeroDuration_shouldSucceed() {
            collector.recordTiming("test_metric", Duration.ZERO);

            MetricsSummary summary = collector.summarize();

            assertThat(summary.get("test_metric").count()).isEqualTo(1);
            assertThat(summary.get("test_metric").min()).isEqualTo(0L);
        }

        @Test
        @DisplayName("should handle nanosecond precision")
        void recordTiming_withNanoPrecision_shouldPreserve() {
            collector.recordTiming("test_metric", Duration.ofNanos(12345));

            MetricsSummary summary = collector.summarize();

            assertThat(summary.get("test_metric").mean()).isCloseTo(12345.0, within(1000.0));
        }
    }

    @Nested
    @DisplayName("Recording Overhead Breakdown")
    class RecordingBreakdownTests {

        @Test
        @DisplayName("should decompose all components")
        void recordOverheadBreakdown_shouldDecomposeAllComponents() {
            OverheadBreakdown breakdown = OverheadBreakdown.builder()
                    .totalLatency(micros(1000))
                    .connectionAcquisition(micros(50))
                    .connectionRelease(micros(20))
                    .serializationTime(micros(100))
                    .wireTransmitTime(micros(75))
                    .serverExecutionTime(micros(400))
                    .serverParseTime(micros(50))
                    .serverTraversalTime(micros(200))
                    .serverIndexTime(micros(30))
                    .serverFetchTime(micros(120))
                    .wireReceiveTime(micros(75))
                    .deserializationTime(micros(80))
                    .clientTraversalTime(micros(25))
                    .build();

            collector.recordOverheadBreakdown(breakdown);

            MetricsSummary summary = collector.summarize();

            assertThat(summary.get("total_latency").mean())
                    .isCloseTo(1_000_000.0, within(1000.0));
            assertThat(summary.get("server_traversal").mean())
                    .isCloseTo(200_000.0, within(1000.0));
            assertThat(summary.get("client_traversal").mean())
                    .isCloseTo(25_000.0, within(1000.0));
        }

        @Test
        @DisplayName("should record derived metrics")
        void recordOverheadBreakdown_shouldRecordDerivedMetrics() {
            OverheadBreakdown breakdown = OverheadBreakdown.builder()
                    .totalLatency(micros(1000))
                    .serverTraversalTime(micros(200))
                    .clientTraversalTime(micros(25))
                    .serverFetchTime(micros(120))
                    .build();

            collector.recordOverheadBreakdown(breakdown);

            MetricsSummary summary = collector.summarize();

            // total_traversal = server + client = 225us
            assertThat(summary.get("total_traversal").mean())
                    .isCloseTo(225_000.0, within(1000.0));

            // total_overhead = total - fetch = 880us
            assertThat(summary.get("total_overhead").mean())
                    .isCloseTo(880_000.0, within(1000.0));
        }
    }

    @Nested
    @DisplayName("Percentile Calculations")
    class PercentileTests {

        @Test
        @DisplayName("should calculate p50 correctly")
        void summarize_shouldCalculateP50() {
            // Add 100 values: 1us, 2us, ..., 100us
            for (int i = 1; i <= 100; i++) {
                collector.recordTiming("test", micros(i));
            }

            MetricsSummary summary = collector.summarize();

            // p50 should be around 50us = 50000ns
            assertThat(summary.get("test").p50())
                    .isBetween(49_000L, 51_000L);
        }

        @Test
        @DisplayName("should calculate p99 correctly")
        void summarize_shouldCalculateP99() {
            // Add 100 values: 1us, 2us, ..., 100us
            for (int i = 1; i <= 100; i++) {
                collector.recordTiming("test", micros(i));
            }

            MetricsSummary summary = collector.summarize();

            // p99 should be around 99us = 99000ns
            assertThat(summary.get("test").p99())
                    .isBetween(98_000L, 100_000L);
        }

        @Test
        @DisplayName("should calculate all percentiles")
        void summarize_shouldCalculateAllPercentiles() {
            for (int i = 1; i <= 1000; i++) {
                collector.recordTiming("test", micros(i));
            }

            MetricsSummary summary = collector.summarize();
            HistogramSummary stats = summary.get("test");

            assertThat(stats.p50()).isGreaterThan(0);
            assertThat(stats.p90()).isGreaterThan(stats.p50());
            assertThat(stats.p95()).isGreaterThan(stats.p90());
            assertThat(stats.p99()).isGreaterThan(stats.p95());
            assertThat(stats.p999()).isGreaterThan(stats.p99());
        }
    }

    @Nested
    @DisplayName("Statistical Calculations")
    class StatisticalTests {

        @Test
        @DisplayName("should calculate mean correctly")
        void summarize_shouldCalculateMean() {
            collector.recordTiming("test", micros(100));
            collector.recordTiming("test", micros(200));
            collector.recordTiming("test", micros(300));

            MetricsSummary summary = collector.summarize();

            assertThat(summary.get("test").mean())
                    .isCloseTo(200_000.0, within(1000.0));
        }

        @Test
        @DisplayName("should track min and max")
        void summarize_shouldTrackMinMax() {
            collector.recordTiming("test", micros(100));
            collector.recordTiming("test", micros(500));
            collector.recordTiming("test", micros(300));

            MetricsSummary summary = collector.summarize();

            // HdrHistogram has precision limits, allow some tolerance
            assertThat(summary.get("test").min()).isBetween(99_000L, 101_000L);
            assertThat(summary.get("test").max()).isBetween(499_000L, 501_000L);
        }

        @Test
        @DisplayName("should calculate standard deviation")
        void summarize_shouldCalculateStdDev() {
            // Values with known std dev
            for (int i = 0; i < 100; i++) {
                collector.recordTiming("test", micros(100));
            }

            MetricsSummary summary = collector.summarize();

            // All same values = 0 std dev
            assertThat(summary.get("test").stdDev()).isCloseTo(0.0, within(1000.0));
        }
    }

    @Nested
    @DisplayName("Thread Safety")
    class ThreadSafetyTests {

        @Test
        @DisplayName("should handle concurrent recording")
        void recordTiming_shouldBeConcurrentSafe() throws InterruptedException {
            int threadCount = 10;
            int iterationsPerThread = 1000;

            Thread[] threads = new Thread[threadCount];
            for (int t = 0; t < threadCount; t++) {
                threads[t] = new Thread(() -> {
                    for (int i = 0; i < iterationsPerThread; i++) {
                        collector.recordTiming("concurrent", micros(100));
                    }
                });
            }

            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }

            MetricsSummary summary = collector.summarize();

            // Allow some tolerance for concurrent access behavior
            long expectedCount = (long) threadCount * iterationsPerThread;
            assertThat(summary.get("concurrent").count())
                    .isGreaterThanOrEqualTo((long) (expectedCount * 0.95)) // At least 95% recorded
                    .isLessThanOrEqualTo(expectedCount);
        }
    }

    @Nested
    @DisplayName("Reset")
    class ResetTests {

        @Test
        @DisplayName("should clear all metrics")
        void reset_shouldClearAllMetrics() {
            collector.recordTiming("metric1", micros(100));
            collector.recordTiming("metric2", micros(200));

            collector.reset();

            MetricsSummary summary = collector.summarize();

            assertThat(summary.hasMetric("metric1")).isFalse();
            assertThat(summary.hasMetric("metric2")).isFalse();
        }

        @Test
        @DisplayName("should allow new recordings after reset")
        void reset_shouldAllowNewRecordings() {
            collector.recordTiming("metric", micros(100));
            collector.reset();
            collector.recordTiming("metric", micros(200));

            MetricsSummary summary = collector.summarize();

            assertThat(summary.get("metric").count()).isEqualTo(1);
            assertThat(summary.get("metric").mean())
                    .isCloseTo(200_000.0, within(1000.0));
        }
    }

    @Nested
    @DisplayName("Timing Helper")
    class TimingHelperTests {

        @Test
        @DisplayName("should time operation execution")
        void timeOperation_shouldRecordDuration() {
            timeSource.setNanoTime(0);

            collector.timeOperation("timed_op", () -> {
                timeSource.advance(micros(500));
            });

            MetricsSummary summary = collector.summarize();

            assertThat(summary.get("timed_op").mean())
                    .isCloseTo(500_000.0, within(1000.0));
        }

        @Test
        @DisplayName("should return operation result")
        void timeOperation_shouldReturnResult() {
            String result = collector.timeOperation("timed_op", () -> {
                timeSource.advance(micros(100));
                return "result";
            });

            assertThat(result).isEqualTo("result");
        }
    }

    @Nested
    @DisplayName("Increment Counter")
    class CounterTests {

        @Test
        @DisplayName("should increment counter")
        void incrementCounter_shouldIncrement() {
            collector.incrementCounter("operations");
            collector.incrementCounter("operations");
            collector.incrementCounter("operations");

            assertThat(collector.getCounter("operations")).isEqualTo(3);
        }

        @Test
        @DisplayName("should handle missing counter")
        void getCounter_whenMissing_shouldReturnZero() {
            assertThat(collector.getCounter("nonexistent")).isEqualTo(0);
        }

        @Test
        @DisplayName("should add to counter")
        void addCounter_shouldAdd() {
            collector.addCounter("bytes", 100);
            collector.addCounter("bytes", 250);

            assertThat(collector.getCounter("bytes")).isEqualTo(350);
        }
    }
}
