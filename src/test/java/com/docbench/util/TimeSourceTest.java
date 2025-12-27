package com.docbench.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static com.docbench.util.TestDurations.micros;
import static org.assertj.core.api.Assertions.*;

/**
 * TDD Tests for TimeSource abstraction.
 * Written BEFORE implementation (RED phase).
 */
@DisplayName("TimeSource")
class TimeSourceTest {

    @Nested
    @DisplayName("SystemTimeSource")
    class SystemTimeSourceTests {

        @Test
        @DisplayName("should return current system time in nanos")
        void nanoTime_shouldReturnSystemNanoTime() {
            TimeSource timeSource = TimeSource.system();

            long before = System.nanoTime();
            long result = timeSource.nanoTime();
            long after = System.nanoTime();

            assertThat(result).isBetween(before, after);
        }

        @Test
        @DisplayName("should return current instant")
        void now_shouldReturnCurrentInstant() {
            TimeSource timeSource = TimeSource.system();

            Instant before = Instant.now();
            Instant result = timeSource.now();
            Instant after = Instant.now();

            assertThat(result).isBetween(before, after);
        }

        @Test
        @DisplayName("should measure elapsed duration accurately")
        void elapsed_shouldMeasureDurationAccurately() throws InterruptedException {
            TimeSource timeSource = TimeSource.system();

            long start = timeSource.nanoTime();
            Thread.sleep(50);
            long end = timeSource.nanoTime();

            Duration elapsed = timeSource.elapsed(start, end);

            assertThat(elapsed.toMillis()).isBetween(45L, 100L);
        }
    }

    @Nested
    @DisplayName("MockTimeSource")
    class MockTimeSourceTests {

        @Test
        @DisplayName("should return controlled nano time")
        void nanoTime_shouldReturnControlledValue() {
            MockTimeSource timeSource = TimeSource.mock(1_000_000L);

            assertThat(timeSource.nanoTime()).isEqualTo(1_000_000L);
        }

        @Test
        @DisplayName("should advance time by specified amount")
        void advance_shouldIncrementNanoTime() {
            MockTimeSource timeSource = TimeSource.mock(0L);

            timeSource.advance(Duration.ofMillis(100));

            assertThat(timeSource.nanoTime()).isEqualTo(100_000_000L);
        }

        @Test
        @DisplayName("should support multiple advances")
        void advance_shouldAccumulate() {
            MockTimeSource timeSource = TimeSource.mock(0L);

            timeSource.advance(Duration.ofMillis(50));
            timeSource.advance(Duration.ofMillis(50));
            timeSource.advance(Duration.ofMillis(50));

            assertThat(timeSource.nanoTime()).isEqualTo(150_000_000L);
        }

        @Test
        @DisplayName("should return controlled instant")
        void now_shouldReturnControlledInstant() {
            Instant fixedInstant = Instant.parse("2025-12-26T10:00:00Z");
            MockTimeSource timeSource = TimeSource.mockAt(fixedInstant);

            assertThat(timeSource.now()).isEqualTo(fixedInstant);
        }

        @Test
        @DisplayName("should advance instant with time")
        void advance_shouldUpdateInstant() {
            Instant fixedInstant = Instant.parse("2025-12-26T10:00:00Z");
            MockTimeSource timeSource = TimeSource.mockAt(fixedInstant);

            timeSource.advance(Duration.ofHours(1));

            assertThat(timeSource.now()).isEqualTo(Instant.parse("2025-12-26T11:00:00Z"));
        }

        @Test
        @DisplayName("should calculate elapsed duration correctly")
        void elapsed_shouldCalculateDuration() {
            MockTimeSource timeSource = TimeSource.mock(0L);

            long start = timeSource.nanoTime();
            timeSource.advance(micros(500));
            long end = timeSource.nanoTime();

            Duration elapsed = timeSource.elapsed(start, end);

            assertThat(elapsed).isEqualTo(micros(500));
        }

        @Test
        @DisplayName("should allow setting specific time")
        void setNanoTime_shouldUpdateTime() {
            MockTimeSource timeSource = TimeSource.mock(0L);

            timeSource.setNanoTime(5_000_000_000L);

            assertThat(timeSource.nanoTime()).isEqualTo(5_000_000_000L);
        }
    }

    @Nested
    @DisplayName("TimingContext")
    class TimingContextTests {

        @Test
        @DisplayName("should capture start time on creation")
        void startTiming_shouldCaptureStartTime() {
            MockTimeSource timeSource = TimeSource.mock(1000L);

            TimeSource.TimingContext context = timeSource.startTiming();

            assertThat(context).isNotNull();
        }

        @Test
        @DisplayName("should calculate elapsed time on stop")
        void stopTiming_shouldReturnElapsedDuration() {
            MockTimeSource timeSource = TimeSource.mock(0L);

            TimeSource.TimingContext context = timeSource.startTiming();
            timeSource.advance(micros(250));
            Duration elapsed = context.stop();

            assertThat(elapsed).isEqualTo(micros(250));
        }

        @Test
        @DisplayName("should support multiple stops returning same duration")
        void stop_shouldBeIdempotent() {
            MockTimeSource timeSource = TimeSource.mock(0L);

            TimeSource.TimingContext context = timeSource.startTiming();
            timeSource.advance(micros(100));

            Duration first = context.stop();
            timeSource.advance(micros(100)); // This should not affect result
            Duration second = context.stop();

            assertThat(first).isEqualTo(second);
        }
    }
}
