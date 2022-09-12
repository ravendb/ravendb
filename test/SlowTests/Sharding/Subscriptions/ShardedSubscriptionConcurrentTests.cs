using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Sharding;
using SlowTests.Client.Subscriptions;
using SlowTests.Core.Utils.Entities;
using SlowTests.Issues;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Subscriptions;

public class ShardedSubscriptionConcurrentTests : RavenTestBase
{
    public ShardedSubscriptionConcurrentTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task ShouldThrow()
    {
        using (var store = Sharding.GetDocumentStore())
        {
            var id = await store.Subscriptions.CreateAsync<User>();

            var e = await Assert.ThrowsAsync<NotSupportedInShardingException>(async () =>
            {
                await using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    Strategy = SubscriptionOpeningStrategy.Concurrent,
                    MaxDocsPerBatch = 2
                }))
                {
                    await subscription.Run(batch => { });
                }
            });

            Assert.Contains("Concurrent subscriptions are not supported in sharding.", e.Message);
        }
    }
}
