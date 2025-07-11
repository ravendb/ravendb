using System;
using System.Threading;
using FastTests;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15746_ThrottledManualResetEventSlimTests : NoDisposalNeeded
    {
        public RavenDB_15746_ThrottledManualResetEventSlimTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void ShouldSetMreImmediatelyWhenThrottlingDisabled()
        {
            using (var mre = new ThrottledManualResetEventSlim(null))
            {

                Assert.Null(mre.ThrottlingInterval);

                mre.Set();

                Assert.True(mre.IsSet);

                mre.Reset();

                Assert.False(mre.IsSet);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void ShouldNotSetMreImmediatelyWhenThrottlingEnabled()
        {
            using (var mre = new ThrottledManualResetEventSlim(TimeSpan.FromHours(1)))
            {
                Assert.Equal(TimeSpan.FromHours(1), mre.ThrottlingInterval);

                mre.Set();

                Assert.False(mre.IsSet);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void WillSetMreOnTime()
        {
            using (var mre = new ThrottledManualResetEventSlim(TimeSpan.FromSeconds(5)))
            {
                mre.Set();

                Assert.True(mre.Wait((int)TimeSpan.FromSeconds(7).TotalMilliseconds, CancellationToken.None));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void CanUpdateThrottlingToDisableIt()
        {
            using (var mre = new ThrottledManualResetEventSlim(TimeSpan.FromSeconds(10)))
            {
                mre.Set();

                mre.Update(null);

                Assert.Null(mre.ThrottlingInterval);
                Assert.True(mre.IsSet);
            }

            using (var mre = new ThrottledManualResetEventSlim(null))
            {
                mre.Update(null);
                Assert.False(mre.IsSet);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void CanUpdateThrottlingToEnableIt()
        {
            using (var mre = new ThrottledManualResetEventSlim(null))
            {
                mre.Update(TimeSpan.FromSeconds(5));

                Assert.Equal(TimeSpan.FromSeconds(5), mre.ThrottlingInterval);

                mre.Set();

                Assert.True(mre.Wait((int)TimeSpan.FromSeconds(7).TotalMilliseconds, CancellationToken.None));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void CanUpdateThrottlingTime()
        {
            using (var mre = new ThrottledManualResetEventSlim(TimeSpan.FromSeconds(3)))
            {
                mre.Update(TimeSpan.FromSeconds(7));

                Assert.Equal(TimeSpan.FromSeconds(7), mre.ThrottlingInterval);

                mre.Set();

                Assert.False(mre.Wait((int)TimeSpan.FromSeconds(4).TotalMilliseconds, CancellationToken.None));

                Assert.True(mre.Wait((int)TimeSpan.FromSeconds(9).TotalMilliseconds, CancellationToken.None));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void CanForceSetByIgnoringThrottling()
        {
            using (var mre = new ThrottledManualResetEventSlim(TimeSpan.FromHours(3)))
            {
                mre.Set();

                Assert.False(mre.IsSet);

                mre.Set(ignoreThrottling: true);

                Assert.True(mre.IsSet);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void CanEnableDisableThrottlingTimer()
        {
            using (var mre = new ThrottledManualResetEventSlim(TimeSpan.FromSeconds(1), timerManagement: ThrottledManualResetEventSlim.TimerManagement.Manual))
            {
                mre.Set();

                Assert.False(mre.Wait((int)TimeSpan.FromSeconds(3).TotalMilliseconds, CancellationToken.None));

                mre.EnableThrottlingTimer();

                Assert.True(mre.Wait((int)TimeSpan.FromSeconds(2).TotalMilliseconds, CancellationToken.None));

                mre.Reset();

                mre.DisableThrottlingTimer();

                Assert.False(mre.IsSet);

                mre.Set();

                Assert.False(mre.Wait((int)TimeSpan.FromSeconds(3).TotalMilliseconds, CancellationToken.None));

                mre.EnableThrottlingTimer();

                Assert.True(mre.Wait((int)TimeSpan.FromSeconds(2).TotalMilliseconds, CancellationToken.None));
            }
        }
    }
}
