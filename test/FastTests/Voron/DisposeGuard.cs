using System;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron
{
    public class DisposeGuard : StorageTest
    {
        public DisposeGuard(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            // Background flushing takes its own low-level transactions, which share the
            // dispose countdown. Manual flushing keeps the countdown owned by this test
            // alone, so both Dispose outcomes are decided by structure, not speed.
            options.ManualFlushing = true;
            options.DisposeWaitTime = TimeSpan.FromMilliseconds(100);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void RefusedDisposeLeavesLiveTransactionsReadable()
        {
            using (var txw = Env.WriteTransaction())
            {
                txw.CreateTree("guarded").Add("key", "value");
                txw.Commit();
            }

            using (var txr = Env.ReadTransaction())
            {
                Assert.Throws<TimeoutException>(() => Env.Dispose());

                var result = txr.ReadTree("guarded").Read("key");
                Assert.NotNull(result);
            }

            // The refused dispose restored the count. With the transaction gone it completes.
            Env.Dispose();
        }
    }
}
