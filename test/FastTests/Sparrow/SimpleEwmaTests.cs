using System;
using System.Threading;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow
{
    public class SimpleEwmaTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Core)]
        public void Seeds_from_first_sample_and_smooths_later_ones()
        {
            var ewma = new SimpleEwma<long>(smoothing: 4);
            Assert.Equal(0, ewma.Current);

            ewma.Update(100);
            Assert.Equal(100, ewma.Current);

            ewma.Update(200);
            Assert.Equal(125, ewma.Current); // 100 + (200 - 100) / 4
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Reading_expires_after_the_validity_window()
        {
            var ewma = new SimpleEwma<long>(smoothing: 4, validityMs: 50);

            ewma.Update(100);
            Assert.Equal(100, ewma.Current);

            WaitForExpiry(ref ewma, validityMs: 50);

            // a stale reading describes the past, not the present - it must read as unseeded
            Assert.Equal(0, ewma.Current);

            // and the next sample re-seeds instead of averaging against the stale regime
            ewma.Update(7);
            Assert.Equal(7, ewma.Current);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void Never_expiring_reading_survives_idleness()
        {
            var ewma = new SimpleEwma<long>(smoothing: 4, validityMs: SimpleEwma.NeverExpires);

            ewma.Update(100);
            Thread.Sleep(100);
            Assert.Equal(100, ewma.Current);
        }

        private static void WaitForExpiry(ref SimpleEwma<long> ewma, long validityMs)
        {
            // sleep can wake early relative to the tick clock; wait until the reading actually expires
            var deadline = Environment.TickCount64 + validityMs * 100;
            while (ewma.Current != 0)
            {
                Assert.True(Environment.TickCount64 < deadline, "the reading never expired");
                Thread.Sleep(10);
            }
        }
    }
}
