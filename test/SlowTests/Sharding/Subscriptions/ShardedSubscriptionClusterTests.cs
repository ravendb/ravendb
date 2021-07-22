using System;
using System.Diagnostics;
using FastTests.Sharding;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Subscriptions
{
    public class ShardedSubscriptionClusterTests : ShardedTestBase
    {
        public ShardedSubscriptionClusterTests(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60);

        //TODO: egor 
    }
}
