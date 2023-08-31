using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_20390 : ReplicationTestBase
    {
        public RavenDB_20390(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Subscriptions)]
        public async Task CanRunSubscriptionWithLastDocumentAfterReplication()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = Sharding.GetDocumentStore())
            {
                var count = 10;
                using (var session = store1.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        session.Store(new User() { Age = i }, $"Users/{i}");
                    }

                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);

                var res = await WaitForValueAsync(() =>
                {
                    using (var session = store2.OpenSession())
                    {
                        return Task.FromResult(session.Query<User>().Count());
                    }
                }, count, interval: 333);

                Assert.Equal(count, res);

                var id = await store2.Subscriptions.CreateAsync(new SubscriptionUpdateOptions()
                {
                    Query = "from 'Users'"
                });
                var cv = Constants.Documents.SubscriptionChangeVectorSpecialStates.LastDocument.ToString();
                await store2.Subscriptions.UpdateAsync(new SubscriptionUpdateOptions() { Name = id, ChangeVector = cv });
                var state = await store2.Subscriptions.GetSubscriptionStateAsync(id);

                using (var subscription = store2.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)))
                {
                    var items = new List<string>();
                    var _ = subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            items.Add(item.Id);
                        }
                    });

                    await CheckShardSubscriptionLastProcessedCVsAsync(store1, store2, state, count);
                    Assert.Empty(items);
                }
            }
        }

        private async Task CheckShardSubscriptionLastProcessedCVsAsync(DocumentStore store1, IDocumentStore store2, SubscriptionState state, int count)
        {
            var tt = await WaitForValueAsync(async () =>
            {
                var shards = Sharding.GetShardsDocumentDatabaseInstancesFor(store2);
                await foreach (var db in shards)
                {
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext docCtx))
                    using (docCtx.OpenReadTransaction())
                    using (db.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext clusterCtx))
                    using (clusterCtx.OpenReadTransaction())
                    {
                        var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(docCtx);
                        var connectionState = db.SubscriptionStorage.GetSubscriptionConnectionsState(clusterCtx, state.SubscriptionName);
                        if (ChangeVectorUtils.GetConflictStatus(databaseChangeVector, connectionState?.LastChangeVectorSent) != ConflictStatus.AlreadyMerged)
                        {
                            return false;
                        }
                    }
                }

                return true;
            }, true, interval: 333);

            Assert.True(tt, "not all shards reached last document");
        }
    }
}
