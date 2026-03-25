using System;
using Raven.Server.Utils.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow
{
    public class MeterMetricAverageTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {        
        [RavenFact(RavenTestCategory.Core)]
        public void AverageDurationUsesSlidingWindow()
        {
            long now = ToNanoseconds(1.0);
            var meter = new MeterMetric(() => now);

            // Ensure average duration is computed over the sliding window and drops back to zero once samples age out.
            meter.Mark(1, 100);

            now = ToNanoseconds(2.0);
            meter.Mark(1, 300);

            now = ToNanoseconds(3.0);

            Assert.Equal(200, meter.GetAverageDuration(TimeSpan.FromSeconds(5)));
            Assert.Equal(300, meter.GetAverageDuration(TimeSpan.FromSeconds(1)));

            now = ToNanoseconds(8.0);
            Assert.Equal(0, meter.GetAverageDuration(TimeSpan.FromSeconds(5)));
        }

        private static long ToNanoseconds(double seconds) => (long)(seconds * Clock.NanosecondsInSecond);
    }
}
