using System;
using Raven.Server.Utils.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow
{
    public class MeterMetricTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Core)]        
        public void BurstThenSilenceAgesOutToZero()
        {
            // Validate that a burst near the end of the ring buffer ages out cleanly and does not leak into later reads.
            
            // Scenario: burst near ring end, then long silence (> MaxWindow).
            // Expectation: windowed rates return zero after silence; new traffic does not resurrect stale samples.
            long now = ToNanoseconds(0.0);
            long NowProvider() => now;
            void Set(double seconds) => now = ToNanoseconds(seconds);

            var meter = new MeterMetric(NowProvider);

            // Burst at t = 1s
            Set(1.0);
            meter.Mark(100);

            // Still within 1s window
            Set(1.5);
            Assert.True(meter.OneSecondRate > 0);

            // After ~15 minutes silence: all windows should yield zero
            Set(1.0 + 15.0 * 60.0);
            Assert.Equal(0.0, meter.OneSecondRate);
            Assert.Equal(0.0, meter.FiveSecondRate);
            Assert.Equal(0.0, meter.OneMinuteRate);
            Assert.Equal(0.0, meter.FiveMinuteRate);

            // New request after silence: rates > 0 but reflect only new traffic
            meter.Mark(1);
            Assert.True(meter.OneSecondRate > 0, "Expected OneSecondRate > 0 after new traffic");
            Assert.True(meter.FiveSecondRate > 0, "Expected FiveSecondRate > 0 after new traffic");
            Assert.True(meter.OneMinuteRate > 0, "Expected OneMinuteRate > 0 after new traffic");
        }
        
        [RavenFact(RavenTestCategory.Core)]        
        public void SlidingWindowHonorsBoundaries()
        {
            long now = ToNanoseconds(1.0);
            long NowProvider() => now;
            void Advance(double seconds) => now += ToNanoseconds(seconds);

            // Verify sliding-window rates respect window boundaries and drop contributions once their bucket expires.
            var meter = new MeterMetric(NowProvider);

            // t = 1s -> 10 requests
            meter.Mark(10);

            // Advance to 2s and add another 5 completions
            Advance(1.0);
            meter.Mark(5);

            Assert.Equal(15, meter.Count);

            // Evaluate rates exactly 2s after the first batch, so only it falls outside the 1s window
            now = ToNanoseconds(3.0);
            var oneSecondRate = meter.OneSecondRate;
            var fiveSecondRate = meter.FiveSecondRate;

            Assert.InRange(oneSecondRate, 4.9, 5.1);
            Assert.InRange(fiveSecondRate, 2.9, 3.1);

            // After 5 more seconds both buckets should age out of the 5s window.
            now = ToNanoseconds(8.0);
            Assert.InRange(meter.FiveSecondRate, 0.0, 0.1);
        }

        private static long ToNanoseconds(double seconds) => (long)(seconds * Clock.NanosecondsInSecond);
    }
}
