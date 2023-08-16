using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Indexing.Benchmark.Entities;
using Raven.Client.Documents.Subscriptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class RavenDB_21028 : InterversionTestBase
    {
        public RavenDB_21028(ITestOutputHelper output) : base(output)
        {
        }

        [MultiplatformFact(RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task CanUse54ClientWith60Subscription()
        {
            var options = InterversionTestOptions.Default;
            await CanUse54ClientWith60SubscriptionInternal(options);
        }

        [MultiplatformFact(RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task CanUse54ClientWith60ShardedSubscription()
        {
            var options = new InterversionTestOptions() { CreateShardedDatabase = true, ReplicationFactor = 1 };
            await CanUse54ClientWith60SubscriptionInternal(options);
        }

        private async Task CanUse54ClientWith60SubscriptionInternal(InterversionTestOptions options)
        {
            // TODO: switch to GetLastStableVersion after release:
            var lastVersion = await GetLastNightlyVersion("6.0");

            using (var store = await GetDocumentStoreAsync(lastVersion, options))
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 128; i++)
                    {
                        await bulkInsert.StoreAsync(new Order { Freight = i }, $"orders/{i}-A");
                    }
                }

                var id = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions() { Query = "from 'Orders'" });
                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                       {
                           TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1)
                       }))
                {
                    var items = new HashSet<string>();
                    subscription.AfterAcknowledgment += batch =>
                    {
                        foreach (var item in batch.Items)
                        {
                            items.Add(item.Id);
                        }

                        return Task.CompletedTask;
                    };

                    var t = subscription.Run(async x => { await Task.Delay(64); });

                    Assert.Equal(128, await WaitForValueAsync(() => items.Count, 128, timeout: 60_000));
                }
            }
        }
    }
}
