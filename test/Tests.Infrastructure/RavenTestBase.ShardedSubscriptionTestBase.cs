using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server;
using Raven.Server.Documents.Commands.Subscriptions;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Xunit;

namespace FastTests;

public partial class RavenTestBase
{
    public class ShardedSubscriptionTestBase
    {
        private readonly RavenTestBase _parent;

        public ShardedSubscriptionTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public async Task AssertNoItemsInTheResendQueueAsync(IDocumentStore store, string subscriptionId, List<RavenServer> servers = null, List<ShardedDocumentDatabase> shards = null)
        {
            var id = long.Parse(subscriptionId);
            shards ??= await _parent.Sharding.GetShardsDocumentDatabaseInstancesFor(store, servers).ToListAsync();
            foreach (var shard in shards)
            {
                if (shard.DatabaseShutdown.IsCancellationRequested)
                    continue;

                var result = await WaitForValueAsync(() =>
                {
                    using (shard.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        return Task.FromResult(AbstractSubscriptionConnectionsState.GetNumberOfResendDocuments(shard.ServerStore, store.Database, SubscriptionType.Document, id));
                    }
                }, 0, timeout: 1_500_000);
                // SubscriptionConnectionsStateBase.TryGetDocumentFromResend()
                using (shard.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.True(result == 0, $"{string.Join(Environment.NewLine, AbstractSubscriptionConnectionsState.GetResendItems(ctx, store.Database, id).Select(x=>$"{x.Id}, {x.ChangeVector}, {x.Batch}"))}");
                }
            }
        }

        public async Task AssertNumberOfItemsInTheResendQueueAsync(IDocumentStore store, string subscriptionId, long expected, List<RavenServer> servers = null,
            List<ShardedDocumentDatabase> shards = null)
        {
            var id = long.Parse(subscriptionId);
            shards ??= await _parent.Sharding.GetShardsDocumentDatabaseInstancesFor(store, servers).ToListAsync();
            foreach (var shard in shards)
            {
                if (shard.DatabaseShutdown.IsCancellationRequested)
                    continue;

                long result = await WaitForValueAsync(() =>
                {
                    using (shard.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        return Task.FromResult(
                            AbstractSubscriptionConnectionsState.GetNumberOfResendDocuments(shard.ServerStore, store.Database, SubscriptionType.Document, id));
                    }
                }, expected, timeout: 60_000);

                using (shard.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    Assert.True(result == expected,
                        $"{string.Join(Environment.NewLine, AbstractSubscriptionConnectionsState.GetResendItems(ctx, store.Database, id).Select(x => $"{x.Id}, {x.ChangeVector}, {x.Batch}"))}");
                }
            }
        }

        public async Task AssertNoOpenSubscriptionConnectionsAsync(IDocumentStore store, string subscriptionId, RavenServer server)
        {
            Assert.Equal(0, await WaitForValueAsync(async () =>
            {
                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    var cmd = new GetSubscriptionConnectionsDetailsCommand(subscriptionId, server.ServerStore.NodeTag);
                    await store.GetRequestExecutor().ExecuteAsync(cmd, context);

                    var res = cmd.Result;
                    return res.Results.Count;
                }
            }, 0, interval: 500));
        }
    }
}
